Install Python 3
    1) Go to https://www.python.org/downloads/
    2) Download Python (push a yellow button)
    3) Run a downloaded file
    4) Make sure a checkbox "Add Python 3.v to PATH" is enabled on installer window before clicking "Install now"
    5) Restart a PC if required.

Install dependencies
    open CMD and execute:
    pip install grequests
    pip install requests
    pip install openpyxl

Execute a script scraper.py in folder Scraper/scraper using IDE, e.g. Pycharm or
double-click the file in folder.

Excel and log files will be created in the same folder where scraper.py is started.


Performance depends on internet connection speed. The best total result I reached is 9 seconds.
You can play with property "gtimeout" if server returns 403 in a line #103 in scraper.py file.
Increasing the property value you can avoid 403 returns. Do short steps - 0.05 or 0.1
