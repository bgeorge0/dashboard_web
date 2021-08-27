from flask import Flask
from flask import render_template
from flask import request
import csv
import dosimetry_dashboard
import MRLerrors_dashboard
import time

app = Flask(__name__)

@app.route('/')
def root():
    return render_template('home.html')

@app.route('/MRLerrors', methods=['GET','POST'])
def handle_MRLerrors_dashboard():
    tic = time.perf_counter()
    SN = request.args.get('SN')
    if SN is None:
        return render_template('MRLerrors_dashboard.html')
        
    # Entry point for path /MRL, supports GET and POST
    if request.method == 'GET':
        # If GET, return the tempalate
        return render_template('MRLerrors_dashboard.html')
    elif request.method == 'POST':
        dashboard_data = [] # Date
        dashboard_format = [] # Formatting
        df_data, df_format = MRLerrors_dashboard.get_MRLdashboard(SN)
        data_csv = df_data.split('\n') # Split into csv and format for datatable ss required
        format_csv = df_format.split('\n')
        reader_data = csv.DictReader(data_csv)        
        for row in reader_data:
            dashboard_data.append(dict(row))
        reader_format = csv.DictReader(format_csv)        
        for row in reader_format:
            dashboard_format.append(dict(row))
        # Extract the fieldnames to pass to the template
        fieldnames = [key for key in dashboard_data[0].keys()]
        print(f"{time.perf_counter() - tic:0.4f} seconds from request to renderer")
        return render_template('MRLerrors_dashboard.html', MRL=dashboard_data, format=dashboard_format, fieldnames=fieldnames, len=len, enumerate=enumerate, zip=zip, str=str)

@app.route('/dosimetry_dashboard', methods=['GET', 'POST'])
def handle_dosimetry_dashboard(): 
    tic = time.perf_counter()    
    Pat_IDA = request.args.get('Pat_IDA')
    if Pat_IDA is None:
        if request.method == 'GET':
            ''' If GET, return the tempalate '''
            return render_template('dosimetry_dashboard.html')
        elif request.method == 'POST':
            ''' If POST, collect dashboard data and return '''
            dashboard_data = [] # Date
            dashboard_format = [] # Formatting
            
            df_data, df_format = dosimetry_dashboard.get_dashboard()
            data_csv = df_data.split('\n') # Split into csv and format for datatable ss required
            format_csv = df_format.split('\n')
            reader_data = csv.DictReader(data_csv)        
            for row in reader_data:
                dashboard_data.append(dict(row))
            reader_format = csv.DictReader(format_csv)        
            for row in reader_format:
                dashboard_format.append(dict(row))
            # Extract the fieldnames to pass to the template
            fieldnames = [key for key in dashboard_data[0].keys()]
            
            print(f"{time.perf_counter() - tic:0.4f} seconds from request to renderer")

            # Render the template, passing the data and funcs as needed
            return render_template('dosimetry_dashboard.html', dashboard=dashboard_data, format=dashboard_format, fieldnames=fieldnames, len=len, enumerate=enumerate, zip=zip, str=str)
    else:
        print(Pat_IDA)
        return render_template('dosimetry_dashboard.html', dashboard=[], format=[], fieldnames=[], len=len, enumerate=enumerate, zip=zip, str=str)

if __name__ == '__main__':
    ''' Entry point '''
    app.run(debug=True, host='0.0.0.0')