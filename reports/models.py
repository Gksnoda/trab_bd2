from django.db import models

class User(models.Model):
    id = models.CharField(primary_key=True, max_length=100)
    display_name = models.CharField(max_length=100)
    broadcaster_type = models.CharField(max_length=50, blank=True)
    description = models.TextField(blank=True)
    profile_image_url = models.URLField(max_length=500, blank=True)
    created_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'users'

class Game(models.Model):
    id = models.CharField(primary_key=True, max_length=100)
    name = models.CharField(max_length=200)
    box_art_url = models.URLField(max_length=500, blank=True)

    class Meta:
        managed = False
        db_table = 'games'

class Stream(models.Model):
    id = models.CharField(primary_key=True, max_length=100)
    user = models.ForeignKey('User', on_delete=models.DO_NOTHING, db_column='user_id')
    game = models.ForeignKey('Game', on_delete=models.DO_NOTHING, db_column='game_id', blank=True, null=True)
    title = models.TextField()
    viewer_count = models.IntegerField(default=0)
    started_at = models.DateTimeField()
    language = models.CharField(max_length=10)
    thumbnail_url = models.URLField(max_length=500)
    tags = models.JSONField(default=list, blank=True)

    class Meta:
        managed = False
        db_table = 'streams'

class Video(models.Model):
    id = models.CharField(primary_key=True, max_length=100)
    stream = models.ForeignKey('Stream', on_delete=models.DO_NOTHING, db_column='stream_id', blank=True, null=True)
    user = models.ForeignKey('User', on_delete=models.DO_NOTHING, db_column='user_id')
    title = models.CharField(max_length=500)
    created_at = models.DateTimeField(null=True, blank=True)
    url = models.URLField(max_length=500)
    view_count = models.BigIntegerField(default=0)
    language = models.CharField(max_length=10)
    duration = models.CharField(max_length=20)

    class Meta:
        managed = False
        db_table = 'videos'

class Clip(models.Model):
    id = models.CharField(primary_key=True, max_length=100)
    url = models.URLField(max_length=500)
    user = models.ForeignKey('User', on_delete=models.DO_NOTHING, db_column='user_id')
    video = models.ForeignKey('Video', on_delete=models.DO_NOTHING, db_column='video_id', blank=True, null=True)
    game = models.ForeignKey('Game', on_delete=models.DO_NOTHING, db_column='game_id', blank=True, null=True)
    language = models.CharField(max_length=10)
    title = models.CharField(max_length=255)
    view_count = models.BigIntegerField(default=0)
    created_at = models.DateTimeField()
    duration = models.FloatField()

    class Meta:
        managed = False
        db_table = 'clips'