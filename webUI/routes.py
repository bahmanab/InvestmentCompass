from flask import Flask, request, session, redirect, url_for
from flask import render_template
from models import db, User, Place, RequestsFileSize, PopularCompanies
from forms import SignupForm, LoginForm, AddressForm, DateTimeForm, CompanyListForm
import os
import datetime

app = Flask(__name__)


app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:'+os.environ["postgres_password"]+'@localhost/edgar'
db.init_app(app)

app.secret_key = 'development-key'


@app.route("/about")    
def about():
    return render_template("about.html")


@app.route("/signup", methods = ['GET', 'POST'])
def signup():
    if 'email' in session:
        return redirect(url_for('home'))

    form = SignupForm()

    if request.method == 'POST':
        if form.validate() == False:
            return render_template('signup.html', form=form)
        else:
            newuser = User(form.first_name.data, form.last_name.data, form.email.data, form.password.data)
            db.session.add(newuser)
            db.session.commit()
            
            session['email'] = newuser.email
            return redirect(url_for('home'))

    elif request.method == 'GET':
        return render_template("signup.html", form=form)


@app.route("/login", methods = ['GET', 'POST'])
def login():
    if 'email' in session:
        return redirect(url_for('home'))

    form = LoginForm()

    if request.method == 'POST':
        if form.validate() == False:
            return render_template('login.html', form=form)
        else:
            email = form.email.data
            password = form.password.data

            user = User.query.filter_by(email=email).first()
            if user is not None and user.check_password(password):
                session['email'] = form.email.data
                return redirect(url_for('home'))
            else:
                return redirect(url_for('login'))

    elif request.method == 'GET':
        return render_template("login.html", form=form) 


@app.route("/logout")
def logout():
    session.pop('email', None)
    return redirect(url_for('index'))   
    

@app.route("/datepicker")
def datepicker():
        return render_template("datepicker.html")


@app.route("/", methods = ['GET', 'POST'])
def home():
    form = DateTimeForm()
    listform = CompanyListForm();

    places = []
    my_coordinates = (37.4221, -122.0844)
    top_companies = ()
    show_chart = False

    if request.method == 'POST':
                        
            # get the date-time range
            date_time_range = form.date_time_range.data
            print(date_time_range)

            # qry = db.session.query(RequestsFileSize).filter(RequestsFileSize.datetime.between('1985-01-17 01:00', '2018-05-05 17:35'))
            # start_date_time, end_date_time = tuple(date_time_range.split(' - '))
            # qry = db.session.query(RequestsFileSize).filter(RequestsFileSize.datetime.between(start_date_time, end_date_time))
            # print(qry)
            # for x in qry:
            #   print(x.datetime, x.size, x.hll)
 
            # query for places around it
            # p = Place()
            # my_coordinates = p.address_to_latlng(address)
            # places = p.query(address)

            # query top companies
            start_date_time = '2017-06-07 02:00'
            end_date_time = '2017-06-20 03:00'
            c = PopularCompanies()
            tops = c.query([start_date_time.strip()+':00', end_date_time.strip()+':00'])
            top_companies = []
            for record in tops:
                top_companies.append(record)
            print("Top companies:", top_companies)
            print("Req:", request.form, request.form)
            value = request.form.getlist('check') 
            if len(value) > 0:
                show_chart = True
            print("Check:", value)

            chartID = 'chart_ID'
            chart_type = 'line'
            chart_height = 500
            chart = {"renderTo": chartID, "type": chart_type, "chart_height": chart_height,}
            series = [{"name": 'AMAZON COM INC', "data": [[int(datetime.datetime.strptime("2013-11-07 00:10:27", "%Y-%m-%d %H:%M:%S").strftime('%s')) * 1000, 1/5],
                                                  [int(datetime.datetime.strptime("2013-11-08 05:20:00", "%Y-%m-%d %H:%M:%S").strftime('%s')) * 1000, 4/5],
                                                  [int(datetime.datetime.strptime("2013-11-10 05:20:00", "%Y-%m-%d %H:%M:%S").strftime('%s')) * 1000, 5/5]
                                                  ]}, 
                      {"name": 'ELECTRONIC ARTS INC.', "data": [[int(datetime.datetime.strptime("2013-11-07 00:10:27", "%Y-%m-%d %H:%M:%S").strftime('%s')) * 1000, 3/5],
                                                  [int(datetime.datetime.strptime("2013-11-08 05:20:00", "%Y-%m-%d %H:%M:%S").strftime('%s')) * 1000, 5/5],
                                                  [int(datetime.datetime.strptime("2013-11-10 05:20:00", "%Y-%m-%d %H:%M:%S").strftime('%s')) * 1000, 2/5]
                                                  ]}]
            title = {"text": 'Popularity Trend Over Time'}
            xAxis = {"type": 'datetime', "dateTimeLabelFormats": {"month": '%e. %b', "year": '%b'}, "title": {"text": 'Date'}}
            yAxis = {"title": {"text": 'Popularity Relative to Most Popular'}, "min": 0}
            tooltip = {"headerFormat": '<b>{series.name}</b><br>', "pointFormat": '{point.x:%e. %b}: {point.y:.2f} m'}
            plotOptions = {"spline": {"marker": {"enabled": "true"}}}
            credits = {"credits": {"enabled": "false"}}
 
            # return those results
            return render_template('home.html', form=form, top_companies=top_companies, listform=listform,
                chartID=chartID, chart=chart, series=series, 
                           title=title, xAxis=xAxis, yAxis=yAxis, tooltip=tooltip, plotOptions=plotOptions,
                           show_chart=show_chart, credits=credits)
 
    elif request.method == 'GET':
        return render_template('home.html', form=form)


if __name__ == "__main__":
    app.run(host='0.0.0.0',debug=True)
