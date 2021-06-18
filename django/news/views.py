from django.shortcuts import render
from django.http import HttpResponse
from django.template import loader

from .models import News


def index(request):
    news = News.objects.all()

    template = loader.get_template('news/index.html')
    context = {
        'news_entities': news,
    }
    return HttpResponse(template.render(context, request))


def detail(request, news_id):
    return HttpResponse("You're looking at news %s." % news_id)
