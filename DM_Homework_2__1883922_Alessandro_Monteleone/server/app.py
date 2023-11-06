from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_query():
    user_input = request.form['user_input']
    # Esegui qui la logica di elaborazione del testo e otieni il risultato
    # Ad esempio, puoi trasformare il testo in maiuscolo:
    result = user_input.upper()
    return render_template('result.html', result=result)



if __name__ == '__main__':
    app.run(debug=True)
