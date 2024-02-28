import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool
import argparse
import pandas

# create an parser object
parser = argparse.ArgumentParser(description='Process input value')

parser.add_argument('--max_pages', type=int, default=5,
                    help='Maximum number of pages to process')
parser.add_argument('--max_worker', type=int, default=5,
                    help='Maximum number of worker')

args = parser.parse_args()

class URLParser:
    def __init__(self, url):
        self.url = url
        self.max_pages = args.max_pages
        self.max_worker = args.max_worker
        self.max_level = 5

    def run(self):
        # generate params
        dt = []
        for i in range(self.max_level):
            tmp = {}
            tmp["level"] = i+1
            tmp["max_page"] = self.max_pages

            dt.append(tmp)
        
        with Pool(5) as pool1:
            results = pool1.map(self.generate_params, dt)
        
        all_params = [item for sublist in results for item in sublist]

        # parallel processing
        with Pool(self.max_worker) as pool:
            results = pool.map(self.parsing_from_url, all_params)

            df = pandas.concat(results, ignore_index=True)
        
        for i in range(self.max_level):
            df[(df["level"] == i+1) & (df["status"] == 200)][["title", "link"]].to_csv(f"datasets/forti_lists_{i+1}.csv", index=False)
        
        df[df["status"] == 400][["level", "page"]].to_json("datasets/skipped.json", orient="records")

            

    def generate_params(self, params):
        lst = []
        for i in range(params["max_page"]):
            tmp_ = {}
            tmp_["level"] = params["level"]
            tmp_["page"] = i+1

            lst.append(tmp_)
        
        return lst

    def parsing_from_url(self, params):
        # create url
        url = f"{self.url}/encyclopedia?type=ips&risk={params['level']}&page={params['page']}"
        try:
            # get response from url
            resp = requests.get(url)

            # create soup
            soup = BeautifulSoup(resp.text, 'html.parser')

            # parsing table-body
            table_body_section = soup.find('section', class_='table-body')

            # get rows from table-body
            rows = table_body_section.find_all('div', class_='row')

            # parsing each row
            dict_ = []
            for row in rows:
                temp_ = {}
                temp_["level"] = params["level"]
                temp_["page"] = params["page"]
                temp_["title"] = row.find("b").text
                temp_["link"] = self.url + row.get('onclick').split(" = ")[1].replace("'","")
                temp_["status"] = 200

                dict_.append(temp_)
            
            results = pandas.DataFrame(dict_)
        except Exception as e:
            print(e)

            # handling error
            results = pandas.DataFrame({
                "level": params["level"],
                "page" : params["page"],
                "title": None,
                "link": None,
                "status": 400
            })
        
        return results

if __name__ == "__main__":
    url = "https://www.fortiguard.com"
    scraper = URLParser(url)
    json_data = scraper.run()