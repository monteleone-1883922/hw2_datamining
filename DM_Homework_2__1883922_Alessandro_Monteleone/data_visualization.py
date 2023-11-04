
import pandas as pd
import subprocess
import webbrowser
import os


import plotly.graph_objects as go


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


def generate_index():
    df = pd.read_csv('DM_Homework_2__1883922_Alessandro_Monteleone/amazon_products_gpu.tsv', sep='\t')
    with open("DM_Homework_2__1883922_Alessandro_Monteleone/server/index.html", "w") as f:
        html_table = df.to_html()
        f.write(HTML_PAGE.format(html_table))

   



if __name__ == "__main__":

    cwd = os.getcwd()
    server_dir = os.path.join(cwd, "server")

    command = START_SERVER  
    process = subprocess.Popen(command, shell=True, text=True, cwd=server_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    webbrowser.open("http://127.0.0.1:8000/")
    # Attendi che il comando venga completato
    output, errore = process.communicate()





    