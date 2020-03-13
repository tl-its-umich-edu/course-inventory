# Canvas Course Inventory

### Installation & Usage

#### With Docker

You can run the application without installing the dependencies manually by leveraging the `Dockerfile` and Docker 
Desktop. To run with Docker, do the following:

1. Build an image.
   ```
   docker build -t inventory .
   ```
1. Run a container using the tagged image, specifying a bind mount to make the output CSVs accessible.
  ```
  docker run --mount type=bind,source={some absolute path to a directory, no trailing slash},target=/data inventory   
  ```

#### With a Virtual Environment

You can also set up the application using `virtualenv` by doing the following:

1. Create a virtual environment using `virtualenv`.
   ```
   virtualenv venv
   source venv/bin/activate  # for Mac OS
   ```
1. Install the dependencies specified in `requirements.txt`.
   ```
   pip install -r requirements.txt
   ```
1. Run the application.
   ```
   python inventory.py
   ```

### Resources

Relevant Canvas API Documentation
* https://canvas.instructure.com/doc/api/accounts.html#method.accounts.courses_api
* https://canvas.instructure.com/doc/api/courses.html#Course