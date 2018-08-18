from flask_wtf import Form
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired, Email, Length


class SignupForm(Form):
	first_name = StringField('First name', validators=[DataRequired(message="Please enter your first name.")])
	last_name = StringField('Last name', validators=[DataRequired("Please enter your last name.")])
	email = StringField('Email', validators=[DataRequired("Please enter your email address."), Email("Please enter your email address.")])
	password = PasswordField('Password', validators=[DataRequired("Please enter your password."), Length(min=6,message="Password should be 6 characters of more.")])
	submit = SubmitField('Sign up')


class LoginForm(Form):
	email = StringField('Email', validators=[DataRequired("Please enter your email address."), Email("Please enter your email address.")])
	password = PasswordField('Password', validators=[DataRequired("Please enter your password.")])
	submit = SubmitField('Sign in')	


class AddressForm(Form):
	address = StringField('Address', validators=[DataRequired("Please enter an address.")])
	submit = SubmitField('Find Top Companies')
	date_time_range = StringField('DateTimeRange')


class DateTimeForm(Form):
	date_time_range = StringField('DateTimeRange')
	submit = SubmitField('Find Top Companies')


class CompanyListForm(Form):
	submit = SubmitField('Plot Popularity Trend', id='plotButton')	
