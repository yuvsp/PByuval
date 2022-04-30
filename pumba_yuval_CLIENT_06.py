#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import urllib.request
from flask import Flask, flash, request, redirect, url_for, render_template,jsonify
from werkzeug.utils import secure_filename
from time import sleep
import json
from json import dumps
from kafka import KafkaProducer
from datetime import datetime
import time
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired, Length
import psycopg2
from psycopg2.extras import RealDictCursor


# In[2]:


app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg', 'gif'])
UPLOAD_FOLDER = 'static/uploads/'


# In[3]:



def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def send_kafka(msg):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))    
    producer.send('img_topic', value=msg)

def connect_db():
    conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="postgres")
    return conn
   
def get_exhibitions_list():
    try:
        cur = connect_db().cursor()
        query = """select "Exhibition" from pg_images"""
        cur.execute(query)
        select = cur.fetchall()
        exhibitions_list = []
        for x in select:
            if x[0] not in exhibitions_list:
                exhibitions_list.append(x[0])
        return exhibitions_list           
    except:
        print("cannot get list of exhibitions from db")
        return []
  
    
def get_exhibition_images(exhibition):
    try:
        cur = connect_db().cursor()
        query = f'select \"Disc_location\" from pg_images where \"Exhibition\" ilike \'{exhibition}\' '
        cur.execute(query)
        select = cur.fetchall()
        images = []
        for x in select:
            if x[0] not in images:
                name = x[0].replace("\\\\","/")
                if os.path.isfile(name):
                    images.append(x[0].replace("\\\\","/"))  
        return(images)
    except:
        return []

def correct_country_for_exhibition(exhibition):
    try:    
        cur = connect_db().cursor()
        query = f"select \"Country\" from pg_images where \"Exhibition\" ilike \'{exhibition}\'"
        cur.execute(query)
        select = cur.fetchone()
        if (select):
            return(select[0])
        else:
            return None
    except:
        return None
   
def get_countries_list():
    try:
        cur = connect_db().cursor()
        query = """select "Country" from pg_images"""
        cur.execute(query)
        select = cur.fetchall()
        countries_list = []
        for x in select:
            if x[0] not in countries_list:
                countries_list.append(x[0])
        return countries_list           
    except:
        print("cannot get list of countries from db")
        return []

def get_country_json(country):
    try:
        cur = connect_db().cursor(cursor_factory=RealDictCursor)
        query = f'select * from pg_images where \"Country\" ilike \'{country}\' '
        cur.execute(query)
        select = cur.fetchall()
        return(json.dumps(select, indent=2))
    except:
        return ''
     

    
@app.route('/')
def upload_form():
    return render_template('baseUpload.html')

@app.route('/', methods=['POST'])
def upload_image():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)
    file = request.files['file']
    if file.filename == '':
        flash('No image selected for uploading')
        return redirect(request.url)
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        Disc_location = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(Disc_location)
        fileDir = os.path.dirname(os.path.realpath('__file__'))
        fullpath = os.path.join(fileDir, Disc_location)
        data = request.form.to_dict()
        data['Disc_location']=Disc_location
        country = data['Country']
        exhibition = data['Exhibition']
        correct_country = correct_country_for_exhibition(exhibition)
        txt = 'Image successfully uploaded and displayed below'
        if correct_country:
            if correct_country != country:
                data['Country'] = correct_country
                txt = f'Exhibition {exhibition} already in DB as in country {correct_country}, not {country}. Saving as {correct_country}. '+txt
        send_kafka(data)
        flash(txt)
        return render_template('baseUpload.html', filename=filename)
    else:
        flash('Allowed image types are -> png, jpg, jpeg, gif')
        return redirect(request.url)

@app.route('/display/<filename>')
def display_image(filename):
    time.sleep(2)
    return redirect(url_for('static', filename='uploads/' + filename), code=301)


@app.route('/exhibitions', methods=['GET','POST'])
def exhibition_images():
    if request.method == 'POST':
        exhibition = request.form.to_dict()['Exhibition']
        images = get_exhibition_images(exhibition)
        return render_template('exhibitionsImages.html', exhibition=exhibition,images=images)
    else:
        exhibitions_list = get_exhibitions_list()
        return render_template('exhibitions.html', exhibitions_list=exhibitions_list)


@app.route('/countries', methods=['GET','POST'])
def country_json_page():
    if request.method == 'POST':
        Country = request.form.to_dict()['Country']
        country_json = get_country_json(Country)
        return render_template('countryJson.html', Country=Country,country_json=country_json)
    else:
        countries_list = get_countries_list()
        return render_template('countries.html', countries_list=countries_list)
    
if __name__ == "__main__":
    app.run(host='0.0.0.0')


# In[ ]:





# In[ ]:





# In[ ]:




