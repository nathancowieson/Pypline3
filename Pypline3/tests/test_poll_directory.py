from unittest2 import TestCase
from Pypline3.poll_directory import PollFileSystem
from Pypline3.visit_id import VisitID



class TestPollFileSystem(TestCase):
    def test_PollFileSystem_with_valid_visit_id(self):
        test_id = 'cm14480-1'
        s = PollFileSystem(VisitID(test_id))
        self.assertEqual(type(s.ReturnNxsFiles()), type([]))

    def test_PollFileSystem_with_invalid_visit_id(self):
        test_id = 'test_id'
        s = PollFileSystem(VisitID(test_id))
        self.assertEqual(s.ReturnNxsFiles(), [])


if __name__ == '__main__':
    unittest2.main()
