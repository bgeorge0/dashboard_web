from flask import Flask
from flask import render_template
from flask import request
import csv
import dosimetry_dashboard
import MRLerrors_dashboard
import time
import plan_details
import pyfiglet

app = Flask(__name__)

@app.errorhandler(Exception)
def server_error(err):
    # Create error page
    ascii_banner = pyfiglet.figlet_format(str(err))
    return render_template('404.html', ascii_banner=ascii_banner)

@app.route('/')
def root():
    ''' Entry point for / '''
    # Return blank template
    return render_template('home.html')

@app.route('/MRLerrors', methods=['GET','POST'])
def handle_MRLerrors_dashboard():
    ''' 
    Entry point for /MRLerrors
    Requires SN as query string to select machine
    '''
    tic = time.perf_counter()
    SN = request.args.get('SN')
    if SN is None:
        # No SN, return blank template
        return render_template('MRLerrors_dashboard.html')

    # If POST, return completed tempalate
    csv_data, csv_format = MRLerrors_dashboard.get_MRLdashboard(SN)
    
    # Format for template
    dashboard_data, dashboard_format = format_csv_for_render(csv_data, csv_format)
    fieldnames = [key for key in dashboard_data[0].keys()] # Extract the fieldnames to pass to the template           
    
    # Timing data
    print(f"{time.perf_counter() - tic:0.4f} seconds from request to renderer")

    # Render the template, passing the data and funcs as needed
    return render_template('MRLerrors_dashboard.html', MRL=dashboard_data, format=dashboard_format, fieldnames=fieldnames, len=len, enumerate=enumerate, zip=zip, str=str)


@app.route('/plan_details', methods=['GET', 'POST'])
def handle_plan_details():
    '''
    Entry point for /plan details.
    Requires GC ID as query string
    '''
    tic = time.perf_counter()
       
    Pat_IDA = request.args.get('Pat_IDA') # Get query string
    if Pat_IDA is None:
        # Return blank template if no GC ID
        return render_template('dosimetry_dashboard.html')

    else:
        # Get correct data based on GC ID
        csv_data, csv_format = plan_details.get_dashboard(Pat_IDA)
        
        # Format for template
        dashboard_data, dashboard_format = format_csv_for_render(csv_data, csv_format)
        fieldnames = [key for key in dashboard_data[0].keys()] # Extract the fieldnames to pass to the template           
            
        # Timing data
        print(f"{time.perf_counter() - tic:0.4f} seconds from request to renderer")

        # Render the template, passing the data and funcs as needed
        return render_template('plan_details.html', dashboard=dashboard_data, format=dashboard_format, fieldnames=fieldnames, len=len, enumerate=enumerate, zip=zip, str=str)


@app.route('/dosimetry_dashboard', methods=['GET', 'POST'])
def handle_dosimetry_dashboard(): 
    ''' Entry point for /dosimetry_dashboard '''
    tic = time.perf_counter() 

    # Get data
    csv_data, csv_format = dosimetry_dashboard.get_dashboard()
    
    # Format for template
    dashboard_data, dashboard_format = format_csv_for_render(csv_data, csv_format)
    fieldnames = [key for key in dashboard_data[0].keys()] # Extract the fieldnames to pass to the template           
    
    print(f"{time.perf_counter() - tic:0.4f} seconds from request to renderer")
    # Render the template, passing the data and funcs as needed
    return render_template('dosimetry_dashboard.html', dashboard=dashboard_data, format=dashboard_format, fieldnames=fieldnames, len=len, enumerate=enumerate, zip=zip, str=str)


def format_csv_for_render(csv_data, csv_format):
    ''' Generic function for format csv of data and format ready for rendering by the templates '''
    dashboard_data = [] # Date
    dashboard_format = [] # Formatting
    data_csv = csv_data.split('\n') # Split into csv and format for datatable ss required
    format_csv = csv_format.split('\n')
    reader_data = csv.DictReader(data_csv)        
    for row in reader_data:
        dashboard_data.append(dict(row))
    reader_format = csv.DictReader(format_csv)        
    for row in reader_format:
        dashboard_format.append(dict(row))
    return dashboard_data, dashboard_format


if __name__ == '__main__':
    ''' Module point '''
    #from . import dashboard_web
    app.run(debug=True, host='0.0.0.0')