import asyncio
import sys
import logging
from collections import defaultdict
from typing import Dict
import json
import os
import time
import datetime
from dotenv import load_dotenv

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from crawl4ai.deep_crawling.filters import (
    FilterChain,
    DomainFilter
)
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("coinness_crawler_extended")

async def run_basic_crawler():
    try:
        # 크롤링 시작 시간 기록
        start_time = time.time()
        start_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # .env 파일 로드
        load_dotenv()
        
        # 대상 URL 설정
        target_url = "https://coinness.com/"
        logger.info(f"시작 URL: {target_url}")
        
        # 필터 체인 - 최소한의 필터만 사용하여 더 많은 페이지를 크롤링
        filter_chain = FilterChain([
            # 도메인 경계 설정 - 오직 coinness.com 도메인으로만 제한
            DomainFilter(
                allowed_domains=["coinness.com"],
                blocked_domains=[]
            )
            # URL 패턴 필터와 콘텐츠 타입 필터는 일단 제거하여 더 넓은 범위의 페이지를 크롤링
        ])

        # 관련성 점수 시스템 - 키워드 확장하고 가중치 증가
        keyword_scorer = KeywordRelevanceScorer(
            keywords=[
                "bitcoin", "crypto", "news", "market", "price", 
                "ethereum", "altcoin", "trading", "analysis", "defi", 
                "nft", "token", "blockchain", "exchange", "coinness"
            ],
            weight=1.0  # 가중치 최대치로 증가
        )

        # 브라우저 설정 - 헤드리스 크롬으로 JavaScript 지원
        browser_config = BrowserConfig(
            browser_type="chromium",
            headless=True,
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )

        # 크롤링 설정 - 뉴스 콘텐츠만 추출
        config = CrawlerRunConfig(
            deep_crawl_strategy=BestFirstCrawlingStrategy(
                max_depth=5,  # 깊이를 3에서 5로 증가
                include_external=False,
                filter_chain=filter_chain,
                url_scorer=keyword_scorer,
                max_pages=50  # 페이지 수를 30에서 50으로 증가
            ),
            # extraction_strategy 제거 - LLM 기반 추출 없음
            stream=True,
            verbose=True,
            cache_mode="bypass",
            wait_until="networkidle",
            page_timeout=90000  # 페이지 타임아웃을 60초에서 90초로 증가
        )

        # AsyncPlaywrightCrawlerStrategy 생성
        crawler_strategy = AsyncPlaywrightCrawlerStrategy(
            browser_config=browser_config
        )

        # 크롤링 실행
        results = []
        score_distribution: Dict[str, int] = defaultdict(int)
        content_types = defaultdict(int)
        extracted_links_count = 0
        
        logger.info("Coinness 확장 크롤링 시작...")
        async with AsyncWebCrawler(crawler_strategy=crawler_strategy) as crawler:
            try:
                # 초기 페이지 크롤링 시작
                logger.debug(f"초기 페이지 크롤링 시작: {target_url}")
                
                # 페이지 탐색 및 링크 추출
                async for result in await crawler.arun(target_url, config=config):
                    # 링크 추출 로깅
                    if hasattr(result, 'links') and result.links:
                        # links가 딕셔너리인 경우 (키가 URL, 값이 속성인 경우)
                        if isinstance(result.links, dict):
                            links_list = list(result.links.keys())
                            extracted_links_count += len(links_list)
                            logger.debug(f"페이지 {result.url}에서 {len(links_list)}개 링크 추출:")
                            # 최대 5개 링크만 표시
                            for i, link in enumerate(links_list[:5] if len(links_list) > 5 else links_list):
                                logger.debug(f"  - 링크 {i+1}: {link}")
                            if len(links_list) > 5:
                                logger.debug(f"  - 그 외 {len(links_list)-5}개 링크...")
                        # links가 리스트인 경우
                        elif isinstance(result.links, list):
                            extracted_links_count += len(result.links)
                            logger.debug(f"페이지 {result.url}에서 {len(result.links)}개 링크 추출:")
                            # 최대 5개 링크만 표시
                            for i, link in enumerate(result.links[:5] if len(result.links) > 5 else result.links):
                                logger.debug(f"  - 링크 {i+1}: {link}")
                            if len(result.links) > 5:
                                logger.debug(f"  - 그 외 {len(result.links)-5}개 링크...")
                        # 다른 형태일 수 있으므로 안전하게 처리
                        else:
                            logger.debug(f"페이지 {result.url}에서 링크가 예상치 않은 형식입니다: {type(result.links)}")
                    else:
                        logger.warning(f"페이지 {result.url}에서 링크를 추출하지 못했습니다.")
                    
                    # 결과가 성공적인지 확인
                    if not result.success:
                        logger.warning(f"실패한 페이지: {result.url}, 원인: {result.error_message}")
                        continue
                    
                    # HTML 내용 로깅
                    html_preview = result.html[:200] if result.html else "없음"
                    logger.debug(f"HTML 미리보기: {html_preview}...")
                    
                    # 페이지 타이틀 추출 (간단한 정규 표현식 사용)
                    title = ""
                    if result.html:
                        import re
                        title_match = re.search(r'<title[^>]*>(.*?)</title>', result.html, re.IGNORECASE | re.DOTALL)
                        if title_match:
                            title = title_match.group(1).strip()
                    
                    # 메타 태그 추출
                    meta_description = ""
                    if result.html:
                        desc_match = re.search(r'<meta\s+name=["|\']description["|\'][^>]*content=["|\']([^"|\']*)["|\']', 
                                              result.html, re.IGNORECASE)
                        if desc_match:
                            meta_description = desc_match.group(1).strip()
                    
                    # HTML 텍스트 추출 (간단한 방법으로)
                    main_text = ""
                    if result.html:
                        # HTML 태그 제거
                        import re
                        clean_text = re.sub(r'<[^>]*>', ' ', result.html)
                        # 여러 공백 제거
                        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                        # 최대 1000자까지만 저장
                        main_text = clean_text[:1000] if len(clean_text) > 1000 else clean_text
                    
                    # 결과 객체에 추가 정보 저장
                    result.metadata["title"] = title
                    result.metadata["description"] = meta_description
                    result.metadata["content_preview"] = html_preview
                    result.metadata["main_text"] = main_text
                    
                    # URL 분석
                    from urllib.parse import urlparse
                    parsed_url = urlparse(result.url)
                    result.metadata["domain"] = parsed_url.netloc
                    result.metadata["path"] = parsed_url.path
                    result.metadata["query"] = parsed_url.query
                        
                    results.append(result)
                    score = result.metadata.get("score", 0)
                    depth = result.metadata.get("depth", 0)
                    
                    # 콘텐츠 타입 분석
                    content_type = "unknown"
                    url_lower = result.url.lower()
                    if "news" in url_lower:
                        content_type = "news"
                    else:
                        continue  # 뉴스가 아닌 경우 건너뜀
                    content_types[content_type] += 1
                    
                    # 스코어 구간별 분포 기록
                    score_range = f"{int(score * 10) / 10:.1f}"
                    score_distribution[score_range] += 1
                    
                    logger.info(f"크롤링 완료: 깊이: {depth} | 점수: {score:.2f} | 유형: {content_type} | URL: {result.url}")
                    
                    # 크롤링 사이에 딜레이 추가 - 서버 부하 및 차단 방지
                    await asyncio.sleep(1.5)  # 딜레이를 1초에서 1.5초로 증가
                    
            except Exception as e:
                logger.error(f"크롤링 중 오류 발생: {str(e)}", exc_info=True)
                # 오류가 발생해도 지금까지 수집한 결과는 분석

        # 결과 분석
        # 크롤링 종료 시간 기록
        end_time = time.time()
        end_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_time = end_time - start_time
        total_time_formatted = time.strftime("%H:%M:%S", time.gmtime(total_time))
        
        logger.info(f"총 추출된 링크 수: {extracted_links_count}")
        logger.info(f"크롤링 소요 시간: {total_time_formatted}")
        
        if not results:
            logger.warning("크롤링된 페이지가 없습니다.")
            return
            
        logger.info(f"총 {len(results)}개의 암호화폐 관련 페이지 크롤링 완료")
        
        if results:
            avg_score = sum(r.metadata.get('score', 0) for r in results) / len(results)
            logger.info(f"평균 점수: {avg_score:.2f}")

            # 깊이별 페이지 수 집계
            depth_counts = defaultdict(int)
            for result in results:
                depth = result.metadata.get("depth", 0)
                depth_counts[depth] += 1

            logger.info("깊이별 크롤링된 페이지:")
            for depth, count in sorted(depth_counts.items()):
                logger.info(f"  깊이 {depth}: {count}개 페이지")
                
            # 점수 분포 분석
            logger.info("점수 분포:")
            for score_range, count in sorted(score_distribution.items(), key=lambda x: float(x[0])):
                percentage = (count / len(results)) * 100
                logger.info(f"  점수 {score_range}: {count}개 페이지 ({percentage:.1f}%)")
            
            # 콘텐츠 유형 분석
            logger.info("콘텐츠 유형 분포:")
            for content_type, count in sorted(content_types.items()):
                percentage = (count / len(results)) * 100
                logger.info(f"  {content_type}: {count}개 페이지 ({percentage:.1f}%)")
                
            # 상위 점수 페이지 출력
            logger.info("상위 5개 고점수 페이지:")
            top_pages = sorted(results, key=lambda r: r.metadata.get('score', 0), reverse=True)[:5]
            for i, page in enumerate(top_pages, 1):
                logger.info(f"  {i}. 점수: {page.metadata.get('score', 0):.2f} | URL: {page.url}")
            
            # 결과 파일로 저장
            try:
                with open('coinness_extended_results.json', 'w', encoding='utf-8') as f:
                    json.dump({
                        "total_pages": len(results),
                        "average_score": avg_score,
                        "depth_distribution": {str(k): v for k, v in depth_counts.items()},
                        "content_types": dict(content_types),
                        "top_urls": [{"url": p.url, "score": p.metadata.get('score', 0)} for p in top_pages],
                        "extracted_links_count": extracted_links_count,
                        # 상세 페이지 정보 추가
                        "pages_details": [
                            {
                                "url": r.url,
                                "title": r.metadata.get("title", ""),
                                "description": r.metadata.get("description", ""),
                                "score": r.metadata.get("score", 0),
                                "depth": r.metadata.get("depth", 0),
                                "content_type": get_content_type(r.url),
                                "content_preview": r.metadata.get("content_preview", "")[:150] + "..." if r.metadata.get("content_preview") else "",
                                "main_text_preview": r.metadata.get("main_text", "")[:200] + "..." if r.metadata.get("main_text") else "",
                                "link_count": len(r.links) if hasattr(r, "links") and r.links else 0,
                                "domain": r.metadata.get("domain", ""),
                                "path": r.metadata.get("path", "")
                            } for r in results
                        ],
                        # 도메인 분석 - 서브도메인 및 경로 분석
                        "domain_analysis": analyze_domains([r.url for r in results]),
                        # 크롤링 통계
                        "statistics": {
                            "start_time": start_datetime,
                            "end_time": end_datetime,
                            "total_time": total_time_formatted,
                            "total_seconds": total_time,
                            "average_page_score": avg_score,
                            "max_score": max([r.metadata.get("score", 0) for r in results]) if results else 0,
                            "min_score": min([r.metadata.get("score", 0) for r in results]) if results else 0,
                            "pages_per_second": len(results) / total_time if total_time > 0 else 0
                        },
                        # 콘텐츠 분석
                        "content_analysis": {
                            "title_keywords": extract_common_keywords([r.metadata.get("title", "") for r in results]),
                            "description_keywords": extract_common_keywords([r.metadata.get("description", "") for r in results]),
                            "content_keywords": extract_common_keywords([r.metadata.get("main_text", "") for r in results]),
                            "average_title_length": sum(len(r.metadata.get("title", "")) for r in results) / len(results) if results else 0,
                            "average_text_length": sum(len(r.metadata.get("main_text", "")) for r in results) / len(results) if results else 0
                        }
                    }, f, indent=2)
                logger.info("크롤링 결과가 coinness_extended_results.json 파일에 저장되었습니다.")
            except Exception as e:
                logger.error(f"결과 저장 중 오류 발생: {str(e)}")
    
    except Exception as e:
        logger.error(f"프로그램 실행 중 예외 발생: {str(e)}", exc_info=True)
        raise

# 콘텐츠 유형 판별을 위한 함수
def get_content_type(url):
    url_lower = url.lower()
    if "news" in url_lower:
        return "news"
    elif "market" in url_lower:
        return "market"
    elif "price" in url_lower:
        return "price"
    elif "article" in url_lower:
        return "article"
    elif "live" in url_lower:
        return "live"
    elif "community" in url_lower:
        return "community"
    else:
        return "unknown"

# URL 분석 함수
def analyze_domains(urls):
    from urllib.parse import urlparse
    
    domain_info = defaultdict(int)
    path_segments = defaultdict(int)
    
    for url in urls:
        parsed = urlparse(url)
        domain = parsed.netloc
        domain_info[domain] += 1
        
        # 경로 세그먼트 분석
        path = parsed.path.strip('/')
        if path:
            segments = path.split('/')
            for segment in segments:
                if segment:
                    path_segments[segment] += 1
    
    return {
        "domains": dict(domain_info),
        "popular_path_segments": dict(sorted(path_segments.items(), key=lambda x: x[1], reverse=True)[:10])
    }

# 키워드 추출 함수
def extract_common_keywords(texts, min_length=2, top_n=20):
    from collections import Counter
    import re
    
    # 불용어 목록
    stop_words = set(['the', 'and', 'in', 'on', 'at', 'to', 'of', 'for', 'with', 'by', 
                      '이', '그', '저', '것', '수', '를', '에', '은', '는', '이', '가', '의', '으로',
                      'this', 'that', 'these', 'those', 'a', 'an'])
    
    # 모든 텍스트 결합
    all_text = ' '.join(texts).lower()
    
    # 단어 추출 (알파벳, 한글, 숫자만)
    words = re.findall(r'[a-zA-Z가-힣0-9]{' + str(min_length) + ',}', all_text)
    
    # 불용어 제거
    filtered_words = [word for word in words if word not in stop_words]
    
    # 빈도수 계산
    word_counts = Counter(filtered_words)
    
    # 상위 N개 단어 반환
    return dict(word_counts.most_common(top_n))

if __name__ == "__main__":
    try:
        asyncio.run(run_basic_crawler())
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 중단되었습니다.")
    except Exception as e:
        logger.error(f"예상치 못한 오류: {str(e)}", exc_info=True)
        sys.exit(1)