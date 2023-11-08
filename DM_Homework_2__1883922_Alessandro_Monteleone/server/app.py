from flask import Flask, render_template, request
from markupsafe import Markup

from amazon_product_analysis import *

app = Flask(__name__)
df = load_data()

@app.route('/')
def indxe():
    return render_template('index.html')


@app.route('/query_menu')
def query_menu():
    table = df.to_html()
    table = Markup(table)
    return render_template('base.html', table=table)

@app.route('/top10price' , methods=['get'])
def top10_prices():
    table = top10(df,"price").to_html()
    table = Markup(table)
    return render_template('fixed_queries.html', table=table)

@app.route('/price_categories' , methods=['get'])
def price_categories():
    table = price_range_for_categories(df[["description","price"]],CATEGORIES).to_html()
    table =  Markup(table)
    return render_template('fixed_queries.html', table=table)
@app.route('/fixed_queries' , methods=['get'])
def fixed_queries():
    return render_template('fixed_queries_menu.html')


@app.route('/query', methods=['POST'])
def process_query():
    user_input = request.form['user_input']
    result = user_input.lower()

    return render_template('result.html', result=result)



if __name__ == '__main__':
    app.run(debug=True)
