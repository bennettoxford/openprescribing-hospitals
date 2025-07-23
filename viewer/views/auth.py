from django.contrib.auth.views import LoginView as AuthLoginView
from ..forms import LoginForm

class LoginView(AuthLoginView):
    template_name = 'login.html'
    form_class = LoginForm
