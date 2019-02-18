

docker-compose up --force-recreate

POST  http://localhost:2000/tasks  - creating new task and returning its id


GET   http://localhost:2000/tasks/{id}  - getting status/information about task with task_id == id