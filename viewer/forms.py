from django.contrib.auth.forms import AuthenticationForm
from django import forms

class LoginForm(AuthenticationForm):
    username = forms.CharField(
        widget=forms.TextInput(attrs={
            'class': 'w-full rounded-md border border-gray-300 shadow-sm focus:border-oxford-500 focus:ring-oxford-500 cursor-text p-1',
        })
    )
    password = forms.CharField(
        widget=forms.PasswordInput(attrs={
            'class': 'w-full rounded-md border border-gray-300 shadow-sm focus:border-oxford-500 focus:ring-oxford-500 cursor-text p-1',
        })
    )