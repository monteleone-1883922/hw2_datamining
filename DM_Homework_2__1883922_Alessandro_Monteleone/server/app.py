from flask import Flask, render_template, request
from markupsafe import Markup

from amazon_product_analysis import *
from utils_and_classes import CATEGORIES, STOPWORDS_FILE_PATH, SPECIAL_CHARACTERS_FILE_PATH

app = Flask(__name__)
df = load_data()
qp = QueryProcessor(INDEX_FILE_PATH, STOPWORDS_FILE_PATH,
                    SPECIAL_CHARACTERS_FILE_PATH)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/query_menu')
def query_menu():
    table = df.to_html()
    table = Markup(table)
    return render_template('base.html', table=table)


@app.route('/top10price', methods=['get'])
def top10_prices():
    table = topN(df, "price", 10).to_html()
    table = Markup(table)
    return render_template('fixed_queries.html', table=table)


@app.route('/top10rated', methods=['get'])
def top10_reviews():
    table = topN(df, "stars", 10).to_html()
    table = Markup(table)
    return render_template('fixed_queries.html', table=table)


@app.route('/top10rated_weighted', methods=['get'])
def top10_reviews_weighted():
    compute_weighted_ratings(df)
    table = topN(df, "weighted_ratings", 10, False).to_html()
    table = Markup(table)
    return render_template('fixed_queries.html', table=table)


@app.route('/price_categories', methods=['get'])
def price_categories():
    table = price_range_for_categories(df[["description", "price"]], CATEGORIES).to_html()
    table = Markup(table)
    return render_template('fixed_queries.html', table=table)


@app.route('/fixed_queries', methods=['get'])
def fixed_queries():
    return render_template('fixed_queries_menu.html')


@app.route('/query', methods=['post'])
def process_query():
    user_input = request.form['user_input']
    heap = qp.query_process(user_input)
    table = get_query_result(df, heap, 10).to_html()
    table = Markup(table)
    return render_template('base.html', table=table)


if __name__ == '__main__':
    app.run(debug=True)
