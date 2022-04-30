#!/usr/bin/env python
# coding: utf-8

# In[1]:

from PIL import Image, ImageEnhance, ImageDraw, ImageFont
from datetime import datetime
from kafka import KafkaConsumer
from json import loads
import psycopg2
import os

now = datetime.now()
print('Server Started')



def init_db():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="postgres")
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS public.pg_images (
            id integer NOT NULL,
            "Name" character varying,
            "Photographer" character varying,
            "Exhibition" character varying,
            "Country" character varying,
            "Style" character varying,
            "Year" integer,
            "Disc_location" character varying
        );
        ALTER TABLE public.pg_images OWNER TO postgres;
        CREATE SEQUENCE public.pg_images_id_seq
            AS integer
            START WITH 1
            INCREMENT BY 1
            NO MINVALUE
            NO MAXVALUE
            CACHE 1;
        ALTER TABLE public.pg_images_id_seq OWNER TO postgres;
        ALTER SEQUENCE public.pg_images_id_seq OWNED BY public.pg_images.id;
        ALTER TABLE ONLY public.pg_images ALTER COLUMN id SET DEFAULT nextval('public.pg_images_id_seq'::regclass);
        ALTER TABLE ONLY public.pg_images
            ADD CONSTRAINT pg_images_pkey PRIMARY KEY (id);""")

        conn.commit()
    except:
        pass
    return conn



conn = init_db()

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="postgres")
cur = conn.cursor()

consumer = KafkaConsumer(
    'img_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))


def process_image(path):
    try:
        file = path
        img = Image.open(file)
        bwImg = img.convert("L")

        img_data = bwImg.getdata()
        pxlList = [255 if i >= 180 else i for i in img_data]

        new_img = Image.new("L", bwImg.size)
        new_img.putdata(pxlList)

        # Find proper font size
        fontsize = 1
        txt = datetime.now().strftime("%d-%m %H:%M")
        img_fraction = 0.50
        font = ImageFont.truetype("arial.ttf", fontsize)
        while font.getsize(txt)[0] < img_fraction * new_img.size[0]:
            fontsize += 1
            font = ImageFont.truetype("arial.ttf", fontsize)
        fontsize -= 1
        font = ImageFont.truetype("arial.ttf", fontsize)

        # Add date text 
        ImageDraw.Draw(new_img).text(font=font, text=txt, xy=(0, 0), fill=(10), stroke_width=1, stroke_fill=200)

        new_img.show()
        new_img.save(path)
        print("okay")
    except:
        print("oy")


# In[ ]:


for message in consumer:
    message = message.value
    query = 'insert into pg_images ' + str(tuple(message.keys())).replace('\'', '\"') + ' values ' + str(
        tuple(message.values()))
    print(query)
    cur.execute(query)
    conn.commit()
    Disc_location = message.get('Disc_location')
    print(Disc_location)
    process_image(Disc_location)

# In[ ]:
