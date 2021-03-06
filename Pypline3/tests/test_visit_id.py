from unittest2 import TestCase
import epics
from Pypline3.visit_id import VisitID

class TestVisitID(TestCase):
    def test_ReturnVisitID_with_manual_set_visit(self):
        test_id = 'test_id'
        s = VisitID(test_id)
        self.assertEqual(s.ReturnVisitID(), test_id)

    def test_ReturnVisitID_with_auto_set_visit(self):
        s = VisitID()
        test_id = epics.PV(s.myconfig['settings']['visit_id_pv']).get()
        self.assertEqual(s.ReturnVisitID(), test_id)

    def test_ReturnSQLDict_with_valid_visit(self):
        test_id = 'cm14480-1'
        s = VisitID(test_id)
        self.assertEqual(s.ReturnSQLDict(), {'visit': ('cm14480-1', '/dls/b21/data/2016/cm14480-1', 2016)})

    def test_ReturnSQLDict_with_invalid_visit(self):
        test_id = 'test_id'
        s = VisitID(test_id)
        self.assertEqual(s.ReturnSQLDict(), False)

    def test_ReturnVisitDirectory_with_valid_visit(self):
        test_id = 'cm14480-1'
        s = VisitID(test_id)
        self.assertEqual(s.ReturnVisitDirectory(), '/dls/b21/data/2016/cm14480-1')

    def test_ReturnVisitDirectory_with_invalid_visit(self):
        test_id = 'test_id'
        s = VisitID(test_id)
        self.assertEqual(s.ReturnVisitDirectory(), False)

if __name__ == '__main__':
    unittest2.main()
