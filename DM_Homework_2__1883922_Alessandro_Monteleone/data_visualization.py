
import pandas as pd
import subprocess
import webbrowser
import os
from amazon_product_analysis import *
from typing import *



HTML_PAGE = """<!DOCTYPE html>
<html>
<head>
    <title>Tabella Tablib</title>
</head>
<body>
    <h1>Tabella generata da Tablib</h1>
    <!-- Includi la tabella HTML generata da Tablib qui -->
    {0}
</body>
</html>
"""
START_SERVER = "python3 -m http.server"


def generate_index_html(df : pd.DataFrame) -> None:
    
    with open("server/index.html", "w") as f:
        html_table = df.to_html()
        f.write(HTML_PAGE.format(html_table))

   

def visualize_table(df : pd.DataFrame) -> None:
    generate_index_html(df)
    cwd = os.getcwd()
    server_dir = os.path.join(cwd, "server")

    command = START_SERVER  
    process = subprocess.Popen(command, shell=True, text=True, cwd=server_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    webbrowser.open("http://127.0.0.1:8000/")
    
    output, errore = process.communicate()




def main():
    df = load_data()
    df_ordered_price = top10(df,"price")
    visualize_table(df_ordered_price)

if __name__ == "__main__":
    main()

    





    