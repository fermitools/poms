class mock_redirect_exception(Exception):
    def __init__(self, url):
        self.url = url

    def what(self):
        return self.url
