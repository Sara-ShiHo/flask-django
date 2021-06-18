from django.db import models


class News(models.Model):
    news_id = models.IntegerField()
    headline = models.TextField(max_length=1000)
    news = models.TextField(max_length=10000)
    news_image = models.TextField(max_length=10000)
    news_url = models.TextField(max_length=1000)

    def __str__(self):
        return '<News id %r>' % self.news_id

