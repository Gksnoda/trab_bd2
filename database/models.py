from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, ForeignKey, BigInteger
from sqlalchemy.orm import relationship
from database.config import Base
from datetime import datetime

class User(Base):
    """
    Modelo para usuários/streamers do Twitch
    """
    __tablename__ = "users"
    
    id = Column(String, primary_key=True)  # Twitch user ID
    login = Column(String(100), nullable=False, unique=True)
    display_name = Column(String(100), nullable=False)
    type = Column(String(20))  # "", "admin", "global_mod", "staff"
    broadcaster_type = Column(String(20))  # "", "affiliate", "partner"
    description = Column(Text)
    profile_image_url = Column(String(500))
    offline_image_url = Column(String(500))
    view_count = Column(BigInteger, default=0)
    created_at = Column(DateTime)
    
    # Relacionamentos
    streams = relationship("Stream", back_populates="user")
    videos = relationship("Video", back_populates="user")
    clips_created = relationship("Clip", foreign_keys="Clip.creator_id", back_populates="creator")
    clips_broadcasted = relationship("Clip", foreign_keys="Clip.broadcaster_id", back_populates="broadcaster")
    
    def __repr__(self):
        return f"<User(id='{self.id}', login='{self.login}', display_name='{self.display_name}')>"

class Game(Base):
    """
    Modelo para jogos/categorias do Twitch
    """
    __tablename__ = "games"
    
    id = Column(String, primary_key=True)  # Twitch game ID
    name = Column(String(200), nullable=False)
    box_art_url = Column(String(500))
    
    # Relacionamentos
    streams = relationship("Stream", back_populates="game")
    clips = relationship("Clip", back_populates="game")
    
    def __repr__(self):
        return f"<Game(id='{self.id}', name='{self.name}')>"

class Stream(Base):
    """
    Modelo para streams ao vivo
    """
    __tablename__ = "streams"
    
    id = Column(String, primary_key=True)  # Twitch stream ID
    user_id = Column(String, ForeignKey('users.id'), nullable=False)
    game_id = Column(String, ForeignKey('games.id'))
    title = Column(String(500))
    viewer_count = Column(Integer, default=0)
    started_at = Column(DateTime)
    language = Column(String(10))
    thumbnail_url = Column(String(500))
    tag_ids = Column(Text)  # JSON string com array de tag IDs
    is_mature = Column(Boolean, default=False)
    
    # Relacionamentos
    user = relationship("User", back_populates="streams")
    game = relationship("Game", back_populates="streams")
    
    def __repr__(self):
        return f"<Stream(id='{self.id}', title='{self.title}', viewer_count={self.viewer_count})>"

class Video(Base):
    """
    Modelo para vídeos (VODs, highlights, uploads)
    """
    __tablename__ = "videos"
    
    id = Column(String, primary_key=True)  # Twitch video ID
    stream_id = Column(String)  # ID da stream original (pode ser null)
    user_id = Column(String, ForeignKey('users.id'), nullable=False)
    title = Column(String(500))
    description = Column(Text)
    created_at = Column(DateTime)
    published_at = Column(DateTime)
    url = Column(String(500))
    thumbnail_url = Column(String(500))
    viewable = Column(String(20))  # "public", "private"
    view_count = Column(BigInteger, default=0)
    language = Column(String(10))
    type = Column(String(20))  # "archive", "highlight", "upload"
    duration = Column(String(20))  # Formato: "1h2m3s"
    
    # Relacionamentos
    user = relationship("User", back_populates="videos")
    clips = relationship("Clip", back_populates="video")
    
    def __repr__(self):
        return f"<Video(id='{self.id}', title='{self.title}', type='{self.type}')>"

class Clip(Base):
    """
    Modelo para clips criados pelos usuários
    """
    __tablename__ = "clips"
    
    id = Column(String, primary_key=True)  # Twitch clip ID
    url = Column(String(500))
    embed_url = Column(String(500))
    broadcaster_id = Column(String, ForeignKey('users.id'), nullable=False)
    creator_id = Column(String, ForeignKey('users.id'), nullable=False)
    video_id = Column(String, ForeignKey('videos.id'))
    game_id = Column(String, ForeignKey('games.id'))
    language = Column(String(10))
    title = Column(String(500))
    view_count = Column(BigInteger, default=0)
    created_at = Column(DateTime)
    thumbnail_url = Column(String(500))
    duration = Column(Integer)  # Duração em segundos
    vod_offset = Column(Integer)  # Offset no vídeo original em segundos
    
    # Relacionamentos
    broadcaster = relationship("User", foreign_keys=[broadcaster_id], back_populates="clips_broadcasted")
    creator = relationship("User", foreign_keys=[creator_id], back_populates="clips_created")
    video = relationship("Video", back_populates="clips")
    game = relationship("Game", back_populates="clips")
    
    def __repr__(self):
        return f"<Clip(id='{self.id}', title='{self.title}', view_count={self.view_count})>" 