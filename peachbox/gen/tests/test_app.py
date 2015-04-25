import unittest
import peachbox.gen
import peachbox.utils
import os.path

class TestApp(unittest.TestCase):

    def setUp(self):
        base_dir = peachbox.utils.TestHelper.mkdir_tmp()
        peachbox.gen.App.PROJECT_HOME = base_dir
        self.app_gen = peachbox.gen.App()
        self.app_gen.create_dirs()
    
    def test_base(self):
        app_gen = peachbox.gen.App()
        assert app_gen.project_home()
        peachbox.gen.App.PROJECT_HOME = '/tmp/base'
        self.assertEqual('/tmp/base', app_gen.project_home())

    def test_create_dirs(self):
        dirs = ['conf', 'setup', 'model', 'pipelines']
        for d in dirs:
            assert os.path.exists(os.path.join(self.app_gen.project_home(), d))

    def test_create_app(self):
        self.app_gen.create_app("MovieReviews")
        assert os.path.exists(os.path.join(self.app_gen.project_home(), "movie_reviews.py"))

    def test_create_model_setup(self):
        self.app_gen.create_model_setup()
        assert os.path.exists(os.path.join(self.app_gen.project_home(), "setup/model.py"))






