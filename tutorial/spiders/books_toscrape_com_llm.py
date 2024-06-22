import json
from json.decoder import JSONDecodeError
from logging import getLogger

import ollama
from html2text import HTML2Text
from litellm import acompletion
from scrapy import Spider

html_cleaner = HTML2Text()
logger = getLogger(__name__)


async def llm_parse(response, prompts):
    key_list = ", ".join(prompts)
    formatted_scheme = "\n".join(f"{k}: {v}" for k, v in prompts.items())
    markdown = html_cleaner.handle(response.text)
    llm_response = await acompletion(
        messages=[
            {
                "role": "user",
                "content": (
                    f"Return a JSON object with the following root keys: "
                    f"{key_list}\n"
                    f"\n"
                    f"Data to scrape:\n"
                    f"{formatted_scheme}\n"
                    f"\n"
                    f"Scrape it from the following Markdown text:\n"
                    f"\n"
                    f"{markdown}"
                ),
            },
        ],
        model="ollama/mistral",
    )
    data = llm_response["choices"][0]["message"]["content"]
    try:
        return json.loads(data)
    except JSONDecodeError:
        logger.error(f"LLM returned an invalid JSON for {response.url}: {data}")
        return {}


class BooksToScrapeComLLMSpider(Spider):
    name = "books_toscrape_com_llm"
    start_urls = [
        "http://books.toscrape.com/catalogue/category/books/mystery_3/index.html"
    ]

    def parse(self, response):
        next_page_links = response.css(".next a")
        yield from response.follow_all(next_page_links)
        book_links = response.css("article a")
        yield from response.follow_all(book_links, callback=self.parse_book)

    async def parse_book(self, response):
        prompts = {
            "name": "Product name",
            "price": "Product price as a number, without the currency symbol",
        }
        llm_data = await llm_parse(response, prompts)
        yield {
            "url": response.url,
            **llm_data,
        }