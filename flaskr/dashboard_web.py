from flask import Flask
from flask import render_template
from flask import request
import csv
import dosimetry_dashboard
import MRLerrors_dashboard
import MRLerrors_graphs
import time
import plan_details
import pyfiglet
import MRL_team
import MRL_completion_fractions
import MRLdelivery_record_analysis
import dosimetry_dashboard_v4

app = Flask(__name__)

'''
@app.errorhandler(Exception)
def server_error(err):
    # Create error page
    ascii_banner = pyfiglet.figlet_format(str(err))
    return render_template('404.html', ascii_banner=ascii_banner)
'''
@app.route('/')
def root():
    ''' Entry point for / '''
    # Return blank template
    return render_template('home.html')

@app.route('/MRL_fractions_graph', methods=['GET','POST'])
def handle_MRL_fractions_graph():
    '''
    Entry point for /MRL_fractions_graph
    '''
    tic = time.perf_counter() 

    # Get data
    fraction_x_data, fraction_y_data = MRLdelivery_record_analysis.MRL_fraction_count()    
    patient_x_data, patient_y_data = MRLdelivery_record_analysis.MRL_patient_count()

    scatter_fraction_data = []
    for x,y in zip(fraction_x_data, fraction_y_data):
        scatter_fraction_data.append({'x': x, 'y': y})
    scatter_fraction_data = str(scatter_fraction_data).replace('\'', '')

    scatter_patient_data = []
    for x,y in zip(patient_x_data, patient_y_data):
        scatter_patient_data.append({'x': x, 'y': y})
    scatter_patient_data = str(scatter_patient_data).replace('\'', '')
       
    print(f"{time.perf_counter() - tic:0.4f} seconds from request to renderer")
    # Render the template, passing the data and funcs as needed
    return render_template('MRL_fractions_graph.html',  scatter_fraction_data=scatter_fraction_data, scatter_patient_data=scatter_patient_data)

@app.route('/MRL_completion_fractions', methods=['GET','POST'])
def handle_MRL_completion_fractions():
    '''
    Entry point for /MRL_completion_fractions
    '''
    tic = time.perf_counter() 

    # Get data
    labels, values = MRL_completion_fractions.get_dashboard()
       
    print(f"{time.perf_counter() - tic:0.4f} seconds from request to renderer")
    # Render the template, passing the data and funcs as needed
    return render_template('MRL_team.html',  values=values, labels=labels)

@app.route('/MRL_team', methods=['GET','POST'])
def handle_MRL_team():
    '''
    Entry point for /MRL_team
    '''
    tic = time.perf_counter() 

    # Get data
    labels, values = MRL_team.get_dashboard()
       
    print(f"{time.perf_counter() - tic:0.4f} seconds from request to renderer")
    # Render the template, passing the data and funcs as needed
    return render_template('MRL_team.html',  values=values, labels=labels)


@app.route('/MRLerrors_graph', methods=['GET','POST'])
def handle_MRLerrors_graph():
    '''
    Entry point for /MRLgraph
    Requries SN as query string to select machine
    '''
    tic = time.perf_counter()
    SN = request.args.get('SN')
    if SN is None:
        # No SN, return blank template
        return render_template('MRLerrors_graph.html')
    
    labels, values = MRLerrors_graphs.get_MRLdashboard(SN)
    
    # Timing data
    print(f"{time.perf_counter() - tic:0.4f} seconds from request to renderer")
    print(labels)
    print(values)

    # Render the template, passing the data and funcs as needed
    return render_template('MRLerrors_graph.html', values=values, labels=labels)


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

@app.route('/MRL_patient_dashboard', methods=['GET', 'POST'])
def handle_MRL_patient_dashboard():
    '''
    Entry point for /MRL_patient_dhasboard
    '''
    tic = time.perf_counter()
    print(f"{time.perf_counter() - tic:0.4f} seconds from request to renderer") # Not very exciting at the moment as it return static images
    return render_template('MRL_patient_dashboard.html')

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

@app.route('/dosimetry_dashboard_debug_schedule', methods=['GET','POST'])
def handle_dosimetry_dashboard_debug_schedule(): 
    ''' Entry point for /dosimetry_dashboard_debug '''
    tic = time.perf_counter() 
    
    sql_query_Schedule_data = '''SELECT * FROM  ScheduleData'''

    # Get data
    csv_data = dosimetry_dashboard.get_custom_query(sql_query_Schedule_data).to_csv()
    
    # Format for template
    dashboard_data, dashboard_format = format_csv_for_render(csv_data, csv_data)
    fieldnames = [key for key in dashboard_data[0].keys()] # Extract the fieldnames to pass to the template           
    print(dashboard_data)
    print(f"{time.perf_counter() - tic:0.4f} seconds from request to renderer")
    # Render the template, passing the data and funcs as needed
    return render_template('dosimetry_dashboard.html', dashboard=dashboard_data, format=dashboard_format, fieldnames=fieldnames, len=len, enumerate=enumerate, zip=zip, str=str)

@app.route('/dosimetry_dashboard_debug_QCL', methods=['GET','POST'])
def handle_dosimetry_dashboard_debug_QCL(): 
    ''' Entry point for /dosimetry_dashboard_debug '''
    tic = time.perf_counter() 

    sql_query_QCL_data = '''SELECT * FROM  QCLData'''

    # Get data
    csv_data = dosimetry_dashboard.get_custom_query(sql_query_QCL_data).to_csv()
    
    # Format for template
    dashboard_data, dashboard_format = format_csv_for_render(csv_data, csv_data)
    fieldnames = [key for key in dashboard_data[0].keys()] # Extract the fieldnames to pass to the template           
    print(dashboard_data)
    print(f"{time.perf_counter() - tic:0.4f} seconds from request to renderer")
    # Render the template, passing the data and funcs as needed
    return render_template('dosimetry_dashboard.html', dashboard=dashboard_data, format=dashboard_format, fieldnames=fieldnames, len=len, enumerate=enumerate, zip=zip, str=str)

@app.route('/kanban_board', methods=['GET','POST'])
def handle_kanban_board():
    ''' Entry point for /kanban_board '''
    tic = time.perf_counter()

    Activity_Filter = request.args.get('Activity')
    steps = ['ROI/Fldsetup','VolumeRendering','InitialDosimetry','1stCheck','2ndCheck','PlanApproval']
    dashboard_data = {}
    dashboard_format = {}
    fieldnames = {}
    for step in steps:
        csv_data, csv_format = dosimetry_dashboard_v4.get_dashboard(Activity_Filter, step)
        # Format for template
        this_dashboard_data, this_dashboard_format = format_csv_for_render(csv_data, csv_format)
        if len(this_dashboard_data) > 0:
            this_fieldnames = [key for key in this_dashboard_data[0].keys()] # Extract the fieldnames to pass to the template
        else:
            this_fieldnames = []
        dashboard_data[step] = this_dashboard_data
        dashboard_format[step] = this_dashboard_format
        fieldnames[step] = this_fieldnames
        
    
    print(f"{time.perf_counter() - tic:0.4f} seconds from request to renderer")
    
    # Render the template, passing the data and funcs as needed
    return render_template('kanban_board.html', steps=steps, dashboard=dashboard_data, format=dashboard_format, fieldnames=fieldnames, len=len, enumerate=enumerate, zip=zip, str=str)

@app.route('/dosimetry_dashboard_v4', methods=['GET', 'POST'])
def handle_dosimetry_dashboard_v4(): 
    ''' Entry point for /dosimetry_dashboard_v4 '''
    tic = time.perf_counter() 

    Activity_Filter = request.args.get('Activity')
    Stage_Filter = request.args.get('Stage')
    csv_data, csv_format = dosimetry_dashboard_v4.get_dashboard(Activity_Filter, Stage_Filter)
    
    # Format for template
    dashboard_data, dashboard_format = format_csv_for_render(csv_data, csv_format)
    fieldnames = [key for key in dashboard_data[0].keys()] # Extract the fieldnames to pass to the template           
    
    print(f"{time.perf_counter() - tic:0.4f} seconds from request to renderer")
    # Render the template, passing the data and funcs as needed
    return render_template('dosimetry_dashboard.html', dashboard=dashboard_data, format=dashboard_format, fieldnames=fieldnames, len=len, enumerate=enumerate, zip=zip, str=str)

@app.route('/dosimetry_dashboard', methods=['GET', 'POST'])
def handle_dosimetry_dashboard(): 
    ''' Entry point for /dosimetry_dashboard '''
    tic = time.perf_counter() 

    Activity_Filter = request.args.get('Activity')
    csv_data, csv_format = dosimetry_dashboard.get_dashboard(Activity_Filter)
    
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
    #from . import dashboard_web
    app.run(debug=True, host='0.0.0.0')#, ssl_context=(r'C:\Users\ben.george\OneDrive - GenesisCare\MRL\Projects\Dashboard-web\flaskr\cert.pem', r'C:\Users\ben.george\OneDrive - GenesisCare\MRL\Projects\Dashboard-web\flaskr\key.pem'))
