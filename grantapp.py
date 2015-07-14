from flask import Flask, render_template
from flask import request, redirect

app = Flask(__name__)

@app.route('/')
def landing():
    author = "Me"
    name = "You"
    return render_template('landing.html', author=author, name=name)

PI = 0
Inst=0

@app.route('/viz', methods = ['POST'])
def piquery():
    PI = request.form['PI']
    print(PI) 
    return render_template('piout.html',subject=PI)

@app.route('/viz2', methods = ['POST'])
def instquery():
    Inst = request.form['Inst']
    print(Inst)
    return render_template('instout.html',subject=Inst)

if __name__ == "__main__":
##    app.debug = True
    app.run()
