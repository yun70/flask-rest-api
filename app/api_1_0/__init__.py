from flask import Blueprint

api = Blueprint('api', __name__)

from . import posts

posts.Load_Taskq()
posts.Load_Jobq()
