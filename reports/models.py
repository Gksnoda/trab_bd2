from django.db import models

class User(models.Model):
    id           = models.CharField(primary_key=True, max_length=100)
    login        = models.CharField(max_length=100)
    display_name = models.CharField(max_length=100)
    type         = models.CharField(max_length=50, blank=True)
    broadcaster_type = models.CharField(max_length=50, blank=True)
    description  = models.TextField(blank=True)
    profile_image_url = models.URLField(max_length=500, blank=True)
    offline_image_url = models.URLField(max_length=500, blank=True)
    view_count   = models.BigIntegerField(default=0)
    created_at   = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'users'


class Game(models.Model):
    id          = models.CharField(primary_key=True, max_length=100)
    name        = models.CharField(max_length=200)
    box_art_url = models.URLField(max_length=500, blank=True)

    class Meta:
        managed = False
        db_table = 'games'


class Stream(models.Model):
    id           = models.CharField(primary_key=True, max_length=100)
    user         = models.ForeignKey(User, on_delete=models.DO_NOTHING, db_column='user_id')
    game         = models.ForeignKey(Game, on_delete=models.DO_NOTHING, db_column='game_id', null=True, blank=True)
    title        = models.TextField()
    viewer_count = models.IntegerField(default=0)
    started_at   = models.DateTimeField()
    language     = models.CharField(max_length=10)
    thumbnail_url= models.URLField(max_length=500)
    tag_ids      = models.JSONField(default=list, blank=True)
    is_mature    = models.BooleanField(default=False)
    collected_at = models.DateTimeField(auto_now_add=True)



    class Meta:
        managed = True
        db_table = 'streams'


class Video(models.Model):
    id            = models.CharField(primary_key=True, max_length=100)
    user          = models.ForeignKey(User, on_delete=models.DO_NOTHING, db_column='user_id')
    stream        = models.ForeignKey(Stream, on_delete=models.DO_NOTHING, db_column='stream_id', null=True, blank=True)
    title         = models.CharField(max_length=500)
    description   = models.TextField(blank=True)
    created_at    = models.DateTimeField(null=True, blank=True)
    published_at  = models.DateTimeField(null=True, blank=True)
    url           = models.URLField(max_length=500)
    thumbnail_url = models.URLField(max_length=500)
    viewable      = models.CharField(max_length=20)
    view_count    = models.BigIntegerField(default=0)
    language      = models.CharField(max_length=10)
    type          = models.CharField(max_length=20)
    duration      = models.CharField(max_length=20)

    class Meta:
        managed = False
        db_table = 'videos'


class Clip(models.Model):
    id             = models.CharField(primary_key=True, max_length=100)
    url            = models.URLField(max_length=500)
    embed_url      = models.URLField(max_length=500)
    broadcaster    = models.ForeignKey(User, on_delete=models.DO_NOTHING, db_column='broadcaster_id', related_name='clips_broadcasted')
    creator        = models.ForeignKey(User, on_delete=models.DO_NOTHING, db_column='creator_id', related_name='clips_created')
    video          = models.ForeignKey(Video, on_delete=models.DO_NOTHING, db_column='video_id', null=True, blank=True)
    game           = models.ForeignKey(Game, on_delete=models.DO_NOTHING, db_column='game_id', null=True, blank=True)
    language       = models.CharField(max_length=10)
    title          = models.CharField(max_length=255)
    view_count     = models.BigIntegerField(default=0)
    created_at     = models.DateTimeField()
    thumbnail_url  = models.URLField(max_length=500)
    duration       = models.FloatField()
    vod_offset     = models.IntegerField(null=True, blank=True)

    class Meta:
        managed = False
        db_table = 'clips'
