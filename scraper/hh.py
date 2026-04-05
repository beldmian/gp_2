from bs4 import BeautifulSoup
from requests import get
import pandas as pd

base_urls = [
    "https://hh.ru/vacancies/analitik-dannyh",
    "https://hh.ru/vacancies/data-scientist",
    "https://hh.ru/vacancies/data-analyst",
    "https://hh.ru/vacancies/analitik",
]


def get_vacancies_ids(url: str, page: int = 0) -> list[int] | None:
    response = get(
        f"{url}?page={page}",
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:148.0) Gecko/20100101 Firefox/148.0"
        },
    )
    if response.status_code != 200:
        return None
    page = BeautifulSoup(response.text)

    ids = []
    for el in page.find_all("div"):
        if not el.has_attr("id"):
            continue
        if not el.attrs["id"].isdigit():
            continue
        ids.append(el.attrs["id"])
    return ids


def get_all_vacancies_ids(urls: list[str]) -> list[int]:
    allIDs = []
    for url in base_urls:
        i = 0
        while True:
            ids = get_vacancies_ids(url, page=i)
            if ids is None:
                break
            print(url, i, len(ids))
            i += 1
            allIDs.extend(ids)
            break
    return allIDs

def get_vacancy_by_id(id: int) -> dict | None:
    print(f"https://hh.ru/vacancy/{id}")
    response = get(
        f"https://hh.ru/vacancy/{id}",
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:148.0) Gecko/20100101 Firefox/148.0"
        },
    )
    if response.status_code != 200:
        return None
    page = BeautifulSoup(response.text)
    titleObj = page.find('div', 'vacancy-title')
    nameObj = None if titleObj is None else titleObj.find('h1')
    salaryObj = page.find('div', attrs={'data-qa': 'vacancy-salary'})
    descriptionObj = page.find('div', attrs={'data-qa': 'vacancy-description'})
    skillsObj = page.find_all("li", attrs={"data-qa": "skills-element"})
    companyObj = page.find('span', 'vacancy-company-name')
    addressObj = page.find('span', attrs={'data-qa': 'vacancy-view-raw-address'})
    return {
        'name': None if nameObj is None else nameObj.text.strip(),
        'salary': None if salaryObj is None else salaryObj.text.strip().replace(u'\xa0', ' '),
        'description': None if descriptionObj is None else descriptionObj.text.strip(),
        'skills': [] if skillsObj is None else list(map(lambda x: x.text.strip(), skillsObj)),
        'company': None if companyObj is None else companyObj.text.strip(),
        'address': None if addressObj is None else addressObj.text.strip(),
    }

data = list(map(get_vacancy_by_id, get_all_vacancies_ids(base_urls)))
df = pd.DataFrame(data)
df.to_csv('./data/hh.csv')