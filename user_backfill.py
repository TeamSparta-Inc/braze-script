from pprint import pprint

import pandas as pd
import requests
import json
import time
from typing import List, Dict, Any
import logging
import boto3
from io import StringIO
import os
import argparse
from dotenv import load_dotenv
import re

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BrazeUserUploader:
    def __init__(self, api_key: str, base_url: str = "https://rest.iad-07.braze.com",
                 aws_profile: str = None, s3_bucket: str = "sparta-braze-currents"):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        self.s3_bucket = s3_bucket

        # S3 클라이언트 초기화
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
        else:
            session = boto3.Session()
        self.s3_client = session.client('s3')

    def validate_phone_number(self, phone: str, identifier: str = "") -> bool:
        """
        전화번호 형식 검증
        - +82로 시작해야 함
        - + 제외하고 숫자만 포함
        - 총 12-13자리 ('+82' + 9-10자리 번호)
        """
        if not phone:
            return True

        # 패턴: +82로 시작, 그 뒤에 9-10자리 숫자
        pattern = r'^\+82\d{9,10}$'

        if not re.match(pattern, phone):
            logger.warning(
                f"잘못된 전화번호 형식 감지 - {identifier}: '{phone}' "
                f"(예상 형식: +82XXXXXXXXX)"
            )
            return False

        return True

    def read_csv_from_s3(self, s3_key: str, dtype: dict = {}) -> pd.DataFrame:
        """
        S3에서 CSV 파일을 읽어서 DataFrame으로 반환
        """
        logger.info(f"S3에서 CSV 파일 읽기 시작: s3://{self.s3_bucket}/{s3_key}")

        try:
            # S3에서 파일 읽기
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            csv_content = response['Body'].read().decode('utf-8')

            # DataFrame으로 변환
            df = pd.read_csv(StringIO(csv_content), dtype=dtype)
            logger.info(f"총 {len(df)}개의 사용자 데이터를 읽었습니다.")
            return df

        except Exception as e:
            logger.error(f"S3에서 파일 읽기 실패: {str(e)}")
            raise

    def convert_csv_to_braze_format(self, csv_source: str, from_s3: bool = True) -> List[Dict[str, Any]]:
        """
        CSV 파일을 읽어서 Braze API 형식으로 변환
        csv_source: S3 키(from_s3=True) 또는 로컬 파일 경로(from_s3=False)
        """
        dtype = {'birthyear': str, 'birthday': str, 'phone': str}

        if from_s3:
            df = self.read_csv_from_s3(csv_source, dtype)
        else:
            logger.info(f"로컬 CSV 파일 읽기 시작: {csv_source}")
            df = pd.read_csv(csv_source, dtype=dtype)
            logger.info(f"총 {len(df)}개의 사용자 데이터를 읽었습니다.")

        braze_attributes = []

        for index, row in df.iterrows():
            try:
                # 기본 속성 구성
                attributes = {}

                # 필수 식별자 (email 또는 external_id 중 하나)
                if pd.notna(row.get('email')) and row['email'].strip():
                    attributes['email'] = row['email'].strip()

                if pd.notna(row.get('external_id')) and row['external_id'].strip():
                    attributes['external_id'] = row['external_id'].strip()

                # email과 external_id가 모두 없으면 건너뛰기
                if not attributes.get('email') and not attributes.get('external_id'):
                    logger.warning(f"행 {index + 1}: email과 external_id가 모두 없습니다. 건너뜁니다.")
                    continue

                # 추가 속성들
                if pd.notna(row.get('first_name')):
                    attributes['first_name'] = row['first_name']

                if pd.notna(row.get('phone')):
                    phone_str = str(row['phone'])

                    if phone_str.startswith('010'):
                        phone_str = f"+82{phone_str[1:]}"
                    elif phone_str.startswith('82'):
                        phone_str = f"+{phone_str}"

                    attributes['phone'] = phone_str

                if pd.notna(row.get('user_type')):
                    attributes['user_type'] = row['user_type']

                if pd.notna(row.get('is_marketing')):
                    attributes['is_marketing'] = bool(row['is_marketing'])

                if pd.notna(row.get('signup_date')):
                    attributes['signup_date'] = row['signup_date']

                if pd.notna(row.get('business')):
                    attributes['business'] = row['business']

                # applied_business 처리 (JSON 문자열인 경우 파싱)
                if pd.notna(row.get('applied_business')):
                    applied_business = row['applied_business']
                    if isinstance(applied_business, str) and applied_business.startswith('['):
                        try:
                            # JSON 배열 문자열을 실제 배열로 변환
                            attributes['applied_business'] = json.loads(applied_business)
                        except json.JSONDecodeError:
                            # JSON 파싱 실패시 문자열 배열 형태로 파싱 시도
                            try:
                                # [item1,item2] 형태를 ['item1', 'item2']로 변환
                                clean_str = applied_business.strip('[]')
                                if clean_str:
                                    attributes['applied_business'] = [item.strip().strip('"\'') for item in
                                                                      clean_str.split(',')]
                                else:
                                    attributes['applied_business'] = []
                            except:
                                # 모든 파싱 실패시 문자열 그대로 사용
                                attributes['applied_business'] = applied_business
                    else:
                        attributes['applied_business'] = applied_business

                # in_progress_business 처리
                if pd.notna(row.get('in_progress_business')):
                    in_progress = row['in_progress_business']
                    if isinstance(in_progress, str) and in_progress.startswith('['):
                        try:
                            attributes['in_progress_business'] = json.loads(in_progress)
                        except json.JSONDecodeError:
                            # JSON 파싱 실패시 문자열 배열 형태로 파싱 시도
                            try:
                                clean_str = in_progress.strip('[]')
                                if clean_str:
                                    attributes['in_progress_business'] = [item.strip().strip('"\'') for item in
                                                                          clean_str.split(',')]
                                else:
                                    attributes['in_progress_business'] = []
                            except:
                                attributes['in_progress_business'] = in_progress
                    else:
                        attributes['in_progress_business'] = in_progress

                # completed_business 처리
                if pd.notna(row.get('completed_business')):
                    completed = row['completed_business']
                    if isinstance(completed, str) and completed.startswith('['):
                        try:
                            attributes['completed_business'] = json.loads(completed)
                        except json.JSONDecodeError:
                            # JSON 파싱 실패시 문자열 배열 형태로 파싱 시도
                            try:
                                clean_str = completed.strip('[]')
                                if clean_str:
                                    attributes['completed_business'] = [item.strip().strip('"\'') for item in
                                                                        clean_str.split(',')]
                                else:
                                    attributes['completed_business'] = []
                            except:
                                attributes['completed_business'] = completed
                    else:
                        attributes['completed_business'] = completed

                if pd.notna(row.get('is_test')):
                    attributes['is_test'] = bool(row['is_test'])

                if pd.notna(row.get('kdt_funnel_stage')):
                    attributes['kdt_funnel_stage'] = row['kdt_funnel_stage']

                if pd.notna(row.get('hh_funnel_stage')):
                    attributes['hh_funnel_stage'] = row['hh_funnel_stage']

                if pd.notna(row.get('has_card')):
                    attributes['has_card'] = bool(row['has_card'])

                # dob 처리
                if pd.notna(row.get('birthyear')) and pd.notna(row.get('birthday')):
                    birthyear = str(row['birthyear'])
                    birthday = str(row['birthday'])
                    attributes['dob'] = f"{birthyear}-{birthday[:2]}-{birthday[2:]}"

                braze_attributes.append(attributes)

                # 디버깅을 위한 로그 (처음 5개 행만)
                if index < 5:
                    logger.info(f"행 {index + 1} 변환 결과:")
                    logger.info(f"  - applied_business: {attributes.get('applied_business', 'N/A')}")
                    logger.info(f"  - in_progress_business: {attributes.get('in_progress_business', 'N/A')}")
                    logger.info(f"  - completed_business: {attributes.get('completed_business', 'N/A')}")
                    logger.info(f"  - dob: {attributes.get('dob', 'N/A')}")

            except Exception as e:
                logger.error(f"행 {index + 1} 처리 중 오류 발생: {str(e)}")
                continue

        logger.info(f"변환 완료: {len(braze_attributes)}개의 유효한 사용자 속성")
        return braze_attributes

    def upload_users_batch(self, attributes_list: List[Dict[str, Any]], batch_size: int = 50) -> bool:
        """
        사용자 속성을 배치로 업로드
        """
        total_batches = (len(attributes_list) + batch_size - 1) // batch_size
        success_count = 0
        error_count = 0
        failed_batches = []  # 실패한 배치를 저장

        for i in range(0, len(attributes_list), batch_size):
            batch = attributes_list[i:i + batch_size]
            batch_num = (i // batch_size) + 1

            logger.info(f"배치 {batch_num}/{total_batches} 처리 중... ({len(batch)}개 사용자)")

            # 전화번호 형식 검증
            for attr in batch:
                if 'phone' in attr:
                    identifier = attr.get('email') or attr.get('external_id') or f"index_{i + batch.index(attr)}"
                    self.validate_phone_number(attr['phone'], identifier)

            payload = {
                "attributes": batch
            }

            try:
                response = requests.post(
                    f"{self.base_url}/users/track",
                    headers=self.headers,
                    json=payload,
                    timeout=30
                )

                if response.status_code == 201:
                    logger.info(f"배치 {batch_num} 성공적으로 업로드됨")
                    success_count += len(batch)
                else:
                    logger.error(f"배치 {batch_num} 업로드 실패: {response.status_code} - {response.text}")
                    error_count += len(batch)
                    failed_batches.append((batch_num, batch, payload))

                # API 레이트 리미트를 위한 대기 (50ms → 200ms로 증가)
                time.sleep(0.5)

            except requests.exceptions.RequestException as e:
                logger.error(f"배치 {batch_num} 요청 실패: {str(e)}")
                error_count += len(batch)
                failed_batches.append((batch_num, batch, payload))

        # 실패한 배치 재시도
        if failed_batches:
            logger.info(f"\n{len(failed_batches)}개의 실패한 배치를 재시도합니다...")
            retry_success_count = 0
            retry_error_count = 0

            for batch_num, batch, payload in failed_batches:
                logger.info(f"배치 {batch_num} 재시도 중... ({len(batch)}개 사용자)")

                try:
                    response = requests.post(
                        f"{self.base_url}/users/track",
                        headers=self.headers,
                        json=payload,
                        timeout=30
                    )

                    if response.status_code == 201:
                        logger.info(f"배치 {batch_num} 재시도 성공")
                        retry_success_count += len(batch)
                        success_count += len(batch)
                        error_count -= len(batch)
                    else:
                        logger.error(f"배치 {batch_num} 재시도 실패: {response.status_code} - {response.text}")
                        logger.error(f"실패한 배치 {batch_num}의 payload 상세:")
                        logger.error(json.dumps(payload, indent=2, ensure_ascii=False))
                        retry_error_count += len(batch)

                    # API 레이트 리미트를 위한 대기 (재시도 시 더 긴 대기)
                    time.sleep(0.5)

                except requests.exceptions.RequestException as e:
                    logger.error(f"배치 {batch_num} 재시도 요청 실패: {str(e)}")
                    logger.error(f"실패한 배치 {batch_num}의 payload 상세:")
                    logger.error(json.dumps(payload, indent=2, ensure_ascii=False))
                    retry_error_count += len(batch)

            logger.info(f"재시도 완료 - 성공: {retry_success_count}, 실패: {retry_error_count}")

        logger.info(f"전체 업로드 완료 - 성공: {success_count}, 실패: {error_count}")
        return error_count == 0

    def upload_from_csv(self, csv_source: str, batch_size: int = 50, from_s3: bool = True) -> bool:
        """
        CSV 파일에서 사용자 데이터를 읽어서 Braze에 업로드
        csv_source: S3 키(from_s3=True) 또는 로컬 파일 경로(from_s3=False)
        """
        try:
            # CSV를 Braze 형식으로 변환
            braze_attributes = self.convert_csv_to_braze_format(csv_source, from_s3=from_s3)

            if not braze_attributes:
                logger.warning("업로드할 유효한 사용자 데이터가 없습니다.")
                return False

            # 배치로 업로드
            return self.upload_users_batch(braze_attributes, batch_size)

        except Exception as e:
            logger.error(f"CSV 업로드 중 오류 발생: {str(e)}")
            return False


def main():
    # .env 파일 로드
    load_dotenv()

    # Command line arguments 파싱
    parser = argparse.ArgumentParser(description='Braze 사용자 데이터 백필 스크립트')
    parser.add_argument('s3_key', type=str, help='S3 CSV 파일 키 (예: backfill-csv/braze_user.csv)')
    parser.add_argument('--batch-size', type=int, default=50, help='배치 크기 (기본값: 50)')
    parser.add_argument('--local', action='store_true', help='로컬 파일 사용 (S3 대신)')

    args = parser.parse_args()

    # 환경 변수에서 설정 가져오기
    API_KEY = os.getenv('BRAZE_API_KEY')
    if not API_KEY:
        logger.error("BRAZE_API_KEY 환경 변수가 설정되지 않았습니다.")
        return

    AWS_PROFILE = os.getenv('AWS_PROFILE')
    S3_BUCKET = os.getenv('S3_BUCKET', 'sparta-braze-currents')

    # Braze 업로더 생성
    uploader = BrazeUserUploader(
        api_key=API_KEY,
        aws_profile=AWS_PROFILE,
        s3_bucket=S3_BUCKET
    )

    # CSV 파일에서 사용자 데이터 업로드
    logger.info("Braze 사용자 데이터 업로드 시작")
    logger.info(f"파일: {args.s3_key}, 배치 크기: {args.batch_size}, 모드: {'로컬' if args.local else 'S3'}")

    success = uploader.upload_from_csv(
        args.s3_key,
        batch_size=args.batch_size,
        from_s3=not args.local
    )

    if success:
        logger.info("모든 사용자 데이터가 성공적으로 업로드되었습니다.")
    else:
        logger.error("일부 사용자 데이터 업로드에 실패했습니다.")


if __name__ == "__main__":
    main()