# Racing-Post Scraper

Web scraper/crawler that collects details of races, horses, sires/damsires,
trainers and jockeys. All information is collected from the planned weekly races
listed on `racingpost.com`. All details are stored in a `sqlite` local database.

## Instructions

- Open a terminal, clone the repo and `cd` into the downloaded directory
- *(Recommended)* Create and activate a virtual environment
    - `virtualenv venv`
    - Windows - `venv\Scripts\activate`
    - Linux - `venv/bin/activate`
- `pip install -r requirements.txt`
- Execute the `main.py` script
    - Windows - `python racingpost_scraper\main.py`
    - Linux - `python racingpost_scraper/main.py`
- You can additionally run each spider manually using the command
  `scrapy crawl <spider-name>`. Make sure to alter/add a `start_urls` list if
  you are calling spiders manually. You can also manually run spiders from inside
  python files (see `main.py` for examples).
  <br>Spider names:
  - `race-spider`
  - `racecard-spider`
  - `horse-spider`
  - `sire-spider`
  - `trainer-spider`
  - `jockey-spider`
 
  

### Requirements 
- `python 3.x+`
- `pip`
- *(Recommended)* `virtualenv`
- Python packages inside of `requirements.txt`
