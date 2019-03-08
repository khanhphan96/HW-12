import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from flask import Flask, jsonify, render_template


app = Flask(__name__)

engine = create_engine('sqlite:///db/bellybutton.sqlite')
Base = automap_base()
Base.prepare(engine, reflect=True)
OTU = Base.classes.otu

print('Tables found', Base.metadata.tables.keys())

SampleMetadata = Base.classes.sample_metadata
Samples = Base.classes.samples

df_samples = pd.read_sql_table('samples', engine)

@app.route('/test')
def test():
	session = Session(engine)
	data = session.query(SampleMetadata).limit(10).all()
	resp = []
	for row in data:
		resp.append({'SAMPLE': row.sample, 
					 'AGE': row.AGE, 
					 'BBTYPE': row.BBTYPE})
	return jsonify(resp)

@app.route('/samples')
def get_all_samples():
	session = Session(engine)
	sql = session.query(SampleMetadata).statement
	print(sql)
	df = pd.read_sql(sql, engine)
	df = df.rename(columns={'sample': 'SAMPLE'})
	return df.to_json(orient='records')

@app.route('/sample_detail/<sample_id>')
def get_sample_detail(sample_id):
	cols = ['otu_id', 'otu_label', sample_id]
	return df_samples[cols].sort_values(sample_id, ascending=False).rename(columns={sample_id: "value"}).to_json(orient='records')


@app.route('/')
def index():
	return render_template('index.html')

if __name__ == '__main__':
	app.run(debug=True, port=5001)

