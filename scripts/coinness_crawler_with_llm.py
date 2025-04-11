import asyncio
import sys
import logging
from collections import defaultdict
from typing import Dict
import json
import os
from dotenv import load_dotenv

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig, LLMConfig
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from crawl4ai.deep_crawling.filters import (
    FilterChain,
    DomainFilter
)
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
from crawl4ai.extraction_strategy import LLMExtractionStrategy

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("coinness_crawler")

async def run_advanced_crawler():
    try:
        # .env 파일 로드 (이미 crawl4ai에서 로드했을 수 있지만 확실하게 하기 위해 다시 로드)
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

        # 관련성 점수 시스템 - 키워드 단순화하고 가중치 증가
        keyword_scorer = KeywordRelevanceScorer(
            keywords=[
                "bitcoin", "crypto", "news", "market", "price"
            ],
            weight=1.0  # 가중치 최대치로 증가
        )

        # LLM 구성 - 암호화폐 관련 정보 추출을 위한 설정
        llm_config = LLMConfig(
            provider="anthropic/claude-3-haiku-20240307",  # OpenAI에서 Anthropic으로 변경
            api_token=os.getenv("ANTHROPIC_API_KEY")  # OpenAI에서 Anthropic API 키로 변경
        )
        
        # LLM 추출 전략 구성 - 암호화폐 관련 정보 추출
        extraction_strategy = LLMExtractionStrategy(
            llm_config=llm_config,
            instruction="""
            이 웹페이지에서 다음 암호화폐 관련 정보를 추출해주세요:
            1. 암호화폐 가격 정보 (비트코인, 이더리움 등)
            2. 주요 뉴스 제목과 요약
            3. 시장 동향 지표
            4. 주요 이벤트나 발표 내용
            
            가능한 명확하고 정확하게 구조화된 형태로 정보를 추출해주세요.
            """,
            extraction_type="block",
            apply_chunking=True,
            input_format="markdown",  # 마크다운 형식으로 처리
            force_json_response=True,  # JSON 형식의 응답 강제
            verbose=True
        )

        # 브라우저 설정 - 헤드리스 크롬으로 JavaScript 지원
        browser_config = BrowserConfig(
            browser_type="chromium",
            headless=True,
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )

        # 크롤링 설정
        config = CrawlerRunConfig(
            deep_crawl_strategy=BestFirstCrawlingStrategy(
                max_depth=2,
                include_external=False,
                filter_chain=filter_chain,
                url_scorer=keyword_scorer,
                max_pages=5  # 처음에는 페이지 수를 적게 설정하여, LLM 처리 비용 최소화
            ),
            extraction_strategy=extraction_strategy,  # LLM 추출 전략 설정
            stream=True,
            verbose=True,
            cache_mode="bypass",
            wait_until="networkidle",
            page_timeout=60000
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
        extracted_data = []  # LLM으로 추출된 데이터를 저장할 리스트
        
        logger.info("Coinness 크롤링 시작...")
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
                    
                    # LLM으로 추출된 데이터 처리
                    if hasattr(result, 'extracted_data') and result.extracted_data:
                        logger.info(f"페이지 {result.url}에서 LLM으로 데이터 추출 성공")
                        extracted_data.append({
                            "url": result.url,
                            "data": result.extracted_data
                        })
                    else:
                        logger.warning(f"페이지 {result.url}에서 LLM 데이터 추출 실패 또는 데이터 없음")
                    
                    # HTML 내용 로깅
                    html_preview = result.html[:200] if result.html else "없음"
                    logger.debug(f"HTML 미리보기: {html_preview}...")
                        
                    results.append(result)
                    score = result.metadata.get("score", 0)
                    depth = result.metadata.get("depth", 0)
                    
                    # 콘텐츠 타입 분석
                    content_type = "unknown"
                    url_lower = result.url.lower()
                    if "news" in url_lower:
                        content_type = "news"
                    elif "market" in url_lower:
                        content_type = "market"
                    elif "price" in url_lower:
                        content_type = "price"
                    content_types[content_type] += 1
                    
                    # 스코어 구간별 분포 기록
                    score_range = f"{int(score * 10) / 10:.1f}"
                    score_distribution[score_range] += 1
                    
                    logger.info(f"크롤링 완료: 깊이: {depth} | 점수: {score:.2f} | 유형: {content_type} | URL: {result.url}")
                    
                    # 크롤링 사이에 짧은 딜레이 추가
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error(f"크롤링 중 오류 발생: {str(e)}", exc_info=True)
                # 오류가 발생해도 지금까지 수집한 결과는 분석

        # 결과 분석
        logger.info(f"총 추출된 링크 수: {extracted_links_count}")
        logger.info(f"LLM으로 추출된 데이터 수: {len(extracted_data)}")
        
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
            
            # LLM 토큰 사용량 표시 (사용 가능한 경우)
            if hasattr(extraction_strategy, 'show_usage'):
                logger.info("LLM 토큰 사용량:")
                extraction_strategy.show_usage()
            
            # 결과 파일로 저장
            try:
                with open('coinness_crawl_with_llm_results.json', 'w', encoding='utf-8') as f:
                    json.dump({
                        "total_pages": len(results),
                        "average_score": avg_score,
                        "depth_distribution": {str(k): v for k, v in depth_counts.items()},
                        "content_types": dict(content_types),
                        "top_urls": [{"url": p.url, "score": p.metadata.get('score', 0)} for p in top_pages],
                        "extracted_links_count": extracted_links_count,
                        "llm_extracted_data": extracted_data  # LLM으로 추출한 데이터 추가
                    }, f, indent=2)
                logger.info("크롤링 결과가 coinness_crawl_with_llm_results.json 파일에 저장되었습니다.")
            except Exception as e:
                logger.error(f"결과 저장 중 오류 발생: {str(e)}")
    
    except Exception as e:
        logger.error(f"프로그램 실행 중 예외 발생: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        asyncio.run(run_advanced_crawler())
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 중단되었습니다.")
    except Exception as e:
        logger.error(f"예상치 못한 오류: {str(e)}", exc_info=True)
        sys.exit(1)