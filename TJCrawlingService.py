import os
import pymysql
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import random
import re
import time
from fuzzywuzzy import fuzz  # 유사도 측정
import logging

# .env 파일 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TJCrawlingService:
    def __init__(self):
        self.db_host = os.getenv('DB_HOST')
        self.db_user = os.getenv('DB_USER')
        self.db_password = os.getenv('DB_PASSWORD')
        self.db_database = os.getenv('DB_DATABASE')
        self.db_port = 3306

    def setup_db_config(self):
        try:
            db = pymysql.connect(
                host=self.db_host,
                user=self.db_user,
                password=self.db_password,
                database=self.db_database,
                port=self.db_port,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
        except pymysql.MySQLError as e:
            logger.error(f"MySQL 연결 실패: {e}")
            raise

        logger.info("db 연결 성공")
        return db

    def get_chosung(self, text):
        CHOSUNG_LIST = ['ㄱ','ㄲ','ㄴ','ㄷ','ㄸ','ㄹ','ㅁ','ㅂ','ㅃ',
                        'ㅅ','ㅆ','ㅇ','ㅈ','ㅉ','ㅊ','ㅋ','ㅌ','ㅍ','ㅎ']
        result = []

        for char in text:
            if '가' <= char <= '힣':
                code = ord(char) - ord('가')
                chosung_index = code // 588
                result.append(CHOSUNG_LIST[chosung_index])
            else:
                result.append('')  # 비한글은 초성 생략 (또는 ' '도 가능)

        return ''.join(result)
     
    def save_to_db(self, songs):
        try:
            connection = self.setup_db_config()
            cursor = connection.cursor()

            # song_info 테이블에 데이터 삽입
            insert_query = """
                INSERT IGNORE INTO song_info 
                (song_number, song_name, artist_name, is_mr, is_live, song_name_chosung, artist_name_chosung)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            inserted_rows = 0

            for song in songs:
                song_number, song_name, artist_name, is_mr, is_live = song

                # 초성 추출
                song_name_chosung = self.get_chosung(song_name)
                artist_name_chosung = self.get_chosung(artist_name)

                cursor.execute(insert_query, (
                    song_number, song_name, artist_name,
                    is_mr, is_live, song_name_chosung, artist_name_chosung
                ))
                if cursor.rowcount > 0:
                    inserted_rows += cursor.rowcount

            connection.commit()
            cursor.close()
            connection.close()

            logger.info(f"{inserted_rows}개의 신곡 정보가 성공적으로 데이터베이스에 저장 되었습니다.")
        except Exception as e:
            logger.error(f"데이터베이스에 저장 중 오류 발생: {e}")
            raise
    
    def read_from_db(self):
        try:
            connection = self.setup_db_config()
            cursor = connection.cursor()

            # 읽기
            query = """
                SELECT song_number FROM song_info
            """
            
            cursor.execute(query)  # 쿼리 실행
            result = cursor.fetchall()  # 결과 가져오기

            cursor.close()
            connection.close()

            return result  # 결과 반환
        except Exception as e:
            logger.error(f"데이터베이스에서 읽기 중 오류 발생: {e}")
            raise
     
    def crawl_new_songs(self):
        try:
            now = datetime.now()
            year = now.strftime("%Y")  # 현재 연도 (YYYY)
            month = now.strftime("%m")  # 현재 달 (MM)
            logger.info(f"{year}년 {month}월의 신곡을 크롤링 시작합니다.")
            url = f"https://m.tjmedia.com/tjsong/song_monthNew.asp?YY={year}&MM={month}"

            response = requests.get(url)
            soup = BeautifulSoup(response.content, "html.parser")

            # 크롤링할 데이터가 들어 있는 태그를 찾아서 반복문으로 처리
            songs = []
            for row in soup.select('tr')[1:]:  # 첫 번째 tr은 헤더이므로 제외
                cols = row.find_all('td')
                if len(cols) >= 3:
                    song_number = cols[0].text.strip()
                    song_name = cols[1].text.strip()
                    artist_name = cols[2].text.strip()
                    songs.append((song_number, song_name, artist_name))
            
            logger.info(f"{len(songs)}개의 신곡 정보가 성공적으로 크롤링되었습니다.")
            logger.info(songs)
            return songs
        except Exception as e:
            logger.error(f"신곡 크롤링 중 오류 발생: {e}")
            raise
    
    def crawl_and_save_new_songs(self):
        try:
            new_songs = self.crawl_new_songs()
            db_song_numbers = self.read_from_db()

            # db에 없는 songs 들만 남긴다
            db_song_numbers_set = {str(song['song_number']) for song in db_song_numbers}  # DB에서 조회한 song_number를 집합으로 변환
            #print(db_song_numbers_set)
            new_songs_filtered = [song for song in new_songs if song[0] not in db_song_numbers_set]
            logger.info(f"DB에 없는 {len(new_songs_filtered)}개의 신곡을 발견했습니다.")
            #print(f"DB에 없는 {len(new_songs_filtered)}개의 신곡을 발견했습니다.")
            if len(new_songs_filtered) == 0:
                return
            # MR 및 Live 정보 크롤링
            songs_include_mr_and_live = self.crawl_mr_and_live(new_songs_filtered)
            # 크롤링 결과가 있는 경우에만 저장 진행
            if songs_include_mr_and_live and len(songs_include_mr_and_live) > 0:
                self.save_to_db(songs_include_mr_and_live)
                # 성공적으로 저장된 곡들에 대해서만 추가 정보 크롤링 진행
                songs_to_process = [song for song in new_songs_filtered if any(s[0] == song[0] for s in songs_include_mr_and_live)]
                if songs_to_process:
                    self.crawl_melon_song_id_and_album(songs_to_process)
                    self.crawl_genre_date_album(songs_to_process)
            else:
                logger.info("크롤링된 MR 및 Live 정보가 없어 저장을 진행하지 않습니다.")
        except Exception as e:
            logger.error(f"신곡 크롤링 및 저장 중 오류 발생: {e}")
            raise

    def crawl_mr_and_live(self, songs):
        try:
            return_songs = []
            for song in songs:
                result = self.crawl_one_mr_and_live(song)
                if result:  # None이 아닌 경우에만 추가
                    return_songs.append(result)
            logger.info(f"{len(return_songs)}개의 MR 및 Live 정보가 성공적으로 크롤링되었습니다.")
            return return_songs
        except Exception as e:
            logger.error(f"MR 및 Live 정보 크롤링 중 오류 발생: {e}")
            raise
    
    def crawl_genre_date_album(self, songs):
        try:
            batch_size = 20
            connection = self.setup_db_config()
            cursor = connection.cursor()

            # song_number 값만 추출하여 SQL IN 조건에 사용할 리스트 생성
            song_numbers = [song[0] for song in songs]  # songs 리스트에 song_number만 추출

            # IN 조건에 사용할 song_number 리스트를 쿼리에 추가
            query = """
                SELECT song_number, song_name, artist_name, melon_song_id 
                FROM song_info 
                WHERE melon_song_id IS NOT NULL 
                AND song_number IN ({})
            """.format(','.join(['%s'] * len(song_numbers)))

            # 쿼리를 실행하여 해당 조건에 맞는 데이터를 가져옴
            cursor.execute(query, song_numbers)
            results = cursor.fetchall()

            for i in range(0, len(results), batch_size):
                batch = results[i:i + batch_size]
                self.process_batch_genre_date_album(batch, cursor, connection)

        except Exception as e:
            logger.error(f"장르, 발매일, 앨범 정보 크롤링 중 오류 발생: {e}")
            raise
        finally:
            cursor.close()
            connection.close()

    def extract_year(self, date_str):
        try:
            # 정규표현식으로 연도 형식 확인 및 추출
            if re.match(r'^\d{4}$', date_str):
                # "2024" 형식이면 그대로 정수로 변환하여 반환
                return int(date_str)
            elif re.match(r'^\d{4}\.\d{2}\.\d{2}$', date_str):
                # "2024.10.28" 형식이면 연도 부분만 추출하여 반환
                return int(date_str.split('.')[0])
            return None  # 연도가 없거나 형식이 맞지 않으면 None 반환
        except Exception as e:
            logger.error(f"Error extracting year from date string {date_str}: {e}")
            return None

    def process_batch_genre_date_album(self, batch, cursor, connection):
        """20개 단위로 멜론 데이터를 BeautifulSoup을 사용해 처리하고 업데이트합니다."""
        headers = {
            "User-Agent": random.choice([
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
                "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
            ])
        }

        for song in batch:
            try:
                song_number = song['song_number']
                melon_song_id = song['melon_song_id']
                song_name = song['song_name']
                artist_name = song['artist_name']

                # 멜론 곡 상세 페이지로 요청
                url = f"https://www.melon.com/song/detail.htm?songId={melon_song_id}"
                response = requests.get(url, headers=headers)

                if response.status_code != 200:
                    logger.error(f"Failed to fetch page for song {song_name} by {artist_name}: Status code {response.status_code}")
                    continue

                # BeautifulSoup으로 HTML 파싱
                soup = BeautifulSoup(response.text, 'html.parser')

                # 장르, 발매일, 앨범 이미지 URL 추출
                try:
                    genre = soup.select_one('dt:contains("장르") + dd').text.strip()
                    release_date = soup.select_one('#downloadfrm > div > div > div:nth-of-type(2) > div:nth-of-type(2) > dl > dd:nth-of-type(2)').text.strip()
                    album_image_url = soup.select_one('#downloadfrm > div > div > div:nth-of-type(1) > a > img')['src']
                except Exception as e:
                    logger.error(f"Error scraping Melon data for song {song_name} by {artist_name}: {e}")
                    continue

                if release_date:
                    release_date = self.extract_year(release_date)

                # 데이터 업데이트
                try:
                    update_query = """
                        UPDATE song_info 
                        SET melon_song_id = %s, genre = %s, year = %s, album = %s
                        WHERE song_number = %s
                    """
                    cursor.execute(update_query, (
                        melon_song_id, genre, release_date, album_image_url, song_number
                    ))
                    connection.commit()
                    logger.info(f"Updated song {song_name} by {artist_name} in the database")
                except Exception as e:
                    logger.error(f"Error updating the database for song {song_name} by {artist_name}: {e}")
                    connection.rollback()

                time.sleep(random.randrange(12, 20))  # 12-20초 랜덤 지연

            except Exception as e:
                logger.error(f"Error processing batch for song {song_name} by {artist_name}: {e}")
                continue
        
    def crawl_one_mr_and_live(self, song):
        try:
            print(f"db에 없는 {song[0]}, {song[1]}, {song[2]} 정보를 추가로 크롤링합니다.")
            song_number = song[0]
            url = 'https://www.tjmedia.com/tjsong/song_search_list.asp?strType=16&natType=&strText='+str(song_number)+'&strCond=1&strSize05=100'

            # POST 요청 보내기
            response = requests.get(url)
            html = response.content.decode('utf-8', 'replace')
            
            # BeautifulSoup으로 HTML 파싱
            soup = BeautifulSoup(html, 'html.parser')
            
            # 해당 위치의 song_number와 일치 여부 확인
            song_number_element = soup.select_one("#BoardType1 > table > tbody > tr:nth-child(2) > td:nth-child(1)")

            # 기본값으로 MR과 Live 설정
            is_mr = False
            is_live = False

            # song_number가 존재하고, 해당 요소의 텍스트와 일치하는지 확인
            if song_number_element and song_number_element.text.strip() == str(song_number):
                logger.info(f"곡 번호 {song_number}와 일치하는 곡이 발견되었습니다.")
                
                # 곡 정보가 담긴 테이블을 찾고, 태그 확인
                song_info = soup.find('table', {'class': 'board_type1'})
                
                # 태그 정보가 있는 경우에만 처리
                if song_info:
                    # "live"와 "mr" 태그가 있는지 확인
                    live_tag = song_info.find_all('img', {'src': '/images/tjsong/live_icon.png'})
                    mr_tag = song_info.find_all('img', {'src': '/images/tjsong/mr_icon.png'})
                    
                    is_live = len(live_tag) > 0
                    is_mr = len(mr_tag) > 0
                else:
                    logger.warning(f"곡 번호 {song_number}의 테이블 정보를 찾을 수 없습니다. 기본값(MR=False, Live=False)으로 설정합니다.")
            else:
                logger.warning(f"곡 번호 {song_number}와 일치하는 곡을 찾을 수 없습니다. 기본값(MR=False, Live=False)으로 설정합니다.")

            # 곡을 찾지 못하더라도 기본 정보는 반환
            return (song_number, song[1], song[2], is_mr, is_live)
            
        except Exception as e:
            logger.error(f"MR 및 Live 정보 크롤링 중 오류 발생: {e}")
            # 예외 발생 시에도 기본값으로 반환
            return (song[0], song[1], song[2], False, False)

    def crawl_melon_song_id_and_album(self, songs):
        try:
            batch_size = 20
            connection = self.setup_db_config()
            cursor = connection.cursor()

            for i in range(0, len(songs), batch_size):
                batch = songs[i:i + batch_size]
                self.process_batch(batch, cursor, connection)
            
        except Exception as e:
            logger.error(f"멜론 곡 ID 및 앨범 이미지 크롤링 중 오류 발생: {e}")
            raise
        finally:
            cursor.close()
            connection.close()

    def find_highest_similarity_match(self, title, artist, results):
        """유사도가 0.6 이상인 항목 중 가장 높은 유사한 항목 선택."""

        def remove_spaces_if_korean(text):
            # 텍스트가 모두 한글인 경우 띄어쓰기를 제거
            if re.fullmatch(r'[가-힣]+', text.replace(" ", "")):
                return text.replace(" ", "")
            return text
    
        def remove_brackets(text):
            # 괄호와 괄호 안의 내용을 제거
            return re.sub(r'\(.*?\)', '', text).strip()

        try:
            valid_matches = []

            # title과 artist에 한국어가 포함된 경우 띄어쓰기 제거
            title = remove_spaces_if_korean(title.strip())
            artist = remove_spaces_if_korean(artist.strip())

            # 유사도 계산 후 0.6 이상인 항목 추가
            for result in results:
                result_song_name, result_artist_name, result_song_id = result
                result_artist_name = remove_brackets(result_artist_name)  # 괄호 안의 내용 제거
                result_song_name = remove_brackets(result_song_name)  # 괄호 안의 내용 제거

                song_name_similarity = fuzz.ratio(title.lower(), result_song_name.lower().strip()) / 100
                artist_name_similarity = fuzz.ratio(artist.lower(), result_artist_name.lower().strip()) / 100
                avg_similarity = (song_name_similarity + artist_name_similarity) / 2

                if song_name_similarity >= 0.5 and artist_name_similarity >= 0.25 and avg_similarity >= 0.5:
                    valid_matches.append((avg_similarity, result_song_name, result_artist_name, result_song_id))
                
                print(f"TJ Title: {title}, Title: {result_song_name}, TJ Artist: {artist}, Arist: {result_artist_name}, Song Name Similarity: {song_name_similarity}, Aritst Similiarity: {artist_name_similarity}, AVG Similarity: {avg_similarity}")

            # 가장 유사도가 높은 항목 선택 (같은 유사도인 경우 첫 번째 항목 사용)
            if valid_matches:
                best_match = max(valid_matches, key=lambda x: x[0])  # 유사도 가장 높은 항목
                highest_similarity = best_match[0]

                # 동일한 유사도가 여러 개일 경우 첫 번째 항목 선택
                for match in valid_matches:
                    if match[0] == highest_similarity:
                        return match  # 가장 먼저 추가된 항목 반환

            # 유사한 항목이 없을 경우 None 반환
            return None
        except Exception as e:
            logger.error(f"Error finding the highest similarity match: {e}")
            return None

    def extract_parentheses_content(self, artist_name):
        """아티스트 이름에서 괄호 안의 내용들 중 'Feat' 관련 내용은 제외하고 나머지를 반환."""

        try:
            # 모든 괄호 안의 내용을 추출
            parentheses_content_list = re.findall(r'\(([^)]*)\)', artist_name)
            
            # 'Feat', 'featuring'이 포함된 내용은 제외한 리스트 생성
            valid_content_list = [content.strip() for content in parentheses_content_list
                                if not any(feat_word in content.lower() for feat_word in ['feat', 'featuring'])]
            
            # 유효한 괄호 내용이 있으면 첫 번째 유효한 내용을 반환, 없으면 None 반환
            if valid_content_list:
                return valid_content_list[0]
            
            return None
        except Exception as e:
            logger.error(f"Error extracting content from parentheses: {e}")
            return None

    def clean_artist_name(self, artist_name):
        """아티스트 이름에서 괄호와 그 안의 내용 제거."""
        return re.sub(r'\([^)]*\)', '', artist_name).strip()  # 괄호와 내용 제거 후 공백 제거

    def process_batch(self, batch, cursor, connection):
        """20개 단위로 멜론 데이터를 BeautifulSoup으로 처리하고 업데이트합니다."""
        
        def search_melon(title, artist):
            """멜론에서 노래와 아티스트로 검색을 수행"""
            headers = {
                "User-Agent": random.choice([
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.110 Safari/537.36",
                    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
                ])
            }
            search_url = f'https://www.melon.com/search/song/index.htm?q={title}+{artist}'
            response = requests.get(search_url, headers=headers)
            time.sleep(random.uniform(1, 3))  # 페이지 로딩 대기

            if response.status_code != 200:
                print(f"Failed to fetch page for {title} by {artist}: Status code {response.status_code}")
                return []

            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 상위 3개의 결과 추출
            rows = soup.select('#frm_defaultList > div > table > tbody > tr')[:3]
            return rows

        for song in batch:
            try:
                print(f"Processing {song[1]} by {song[2]}")
                time.sleep(1)  # 1초 대기 (random delay)

                title = song[1]
                artist = self.clean_artist_name(song[2])  # 아티스트 이름 정리
                current_artist_name = artist
                artist_with_english = self.extract_parentheses_content(song[2])  # 괄호 안의 내용 추출
                song_number = song[0]

                # 1. 기본적으로 제목과 정리된 아티스트 이름으로 검색
                rows = search_melon(title, artist)

                # 3. 여전히 결과가 없으면 괄호 안의 내용(영어 이름)으로 검색
                if not rows and artist_with_english:
                    rows = search_melon(title, artist_with_english)
                    current_artist_name = artist_with_english

                # 4. 여전히 결과가 없으면 원래 아티스트 이름으로 다시 검색
                if not rows:
                    rows = search_melon(title, song[2])
                    current_artist_name = song[2]

                # 5. 최종적으로 결과가 없으면 제목만으로 검색
                if not rows:
                    rows = search_melon(title, "")
                    current_artist_name = artist

                if rows:
                    print(f"Found {len(rows)} results for {title} by {artist}")
                    
                    search_results = []
                    
                    for row in rows:
                        try:
                            # 곡 이름 추출
                            song_name_tag = row.select_one('td:nth-of-type(3) a.fc_gray')
                            song_name = song_name_tag.text.strip() if song_name_tag else None
                            song_id = None

                            # 곡 ID 추출 (JavaScript 함수의 파라미터에서 추출)
                            link_element = row.select_one('td:nth-of-type(3) a.btn_icon_detail')
                            if link_element:
                                href = link_element['href']
                                match = re.search(r"searchLog\('web_song','SONG','SO','([^']+)','(\d+)'\);", href)
                                if match:
                                    song_id = match.group(2)
                                    print(f"Song ID: {song_id}")
                                else:
                                    print(f"No song ID found in the link: {href}")

                            # 아티스트 이름 추출
                            artist_name_tag = row.select_one('td:nth-of-type(4) div > div')
                            artist_name = artist_name_tag.text.strip() if artist_name_tag else None
                            print(f"Song Name: {song_name}, Artist Name: {artist_name}")

                            # 검색 결과에 추가
                            if song_name and artist_name:
                                search_results.append((song_name, artist_name, song_id))
                        except Exception as e:
                            print(f"Error fetching song info: {e}")
                            continue

                    # 가장 유사한 검색 결과 찾기
                    best_match = self.find_highest_similarity_match(title, current_artist_name, search_results)
                    if best_match:
                        _, result_title, result_artist, best_song_id = best_match
                        print(f"Best Match: {result_title} Artist: {result_artist}, Song ID: {best_song_id}")

                        # 데이터베이스에 업데이트
                        update_query = """
                            UPDATE song_info 
                            SET melon_song_id = %s 
                            WHERE song_number = %s
                        """
                        cursor.execute(update_query, (best_song_id, song_number))
                        connection.commit()
                        logger.info(f"Updated Song ID {best_song_id} for {title} by {artist}")
                    else:
                        print(f"No suitable match found for {title} by {artist}")

                else:
                    print(f"No results found for {title} by {artist}")

            except Exception as e:
                logger.error(f"Failed to retrieve the Song ID for {title} by {artist}: {e}")
                continue
            
