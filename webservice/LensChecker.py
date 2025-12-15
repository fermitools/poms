import requests

class LensChecker:
    lens_url="https://landscape.fnal.gov/lens/query"
    query="""{"query":"{submission(pomsTaskID: %s){id submitTime done failed completed running}}"}"""
    headers= {
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/json",
        "Accept": "*/*",
        "Connection": "keep-alive",
        "DNT": "1",
    }

    def __init__(self):
        self.sess = requests.Session()
        self.sess.headers.update(LensChecker.headers)

    def check_submission(self, submission_id):
        r = self.sess.post( LensChecker.lens_url, LensChecker.query % submission_id )
        ddict = r.json()
        r.close()
        return ddict


if __name__ == '__main__':
    import sys
    lc = LensChecker()
    print(lc.check_submission(2449269))
