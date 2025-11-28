"""
Unit tests for vector clock implementation
"""

import unittest
from vector_clock import VectorClock, VersionedValue, reconcile_versions


class TestVectorClock(unittest.TestCase):
    """Test cases for VectorClock class."""
    
    def test_increment(self):
        """Test clock increment."""
        vc = VectorClock()
        vc.increment("node1")
        self.assertEqual(vc.clock["node1"], 1)
        
        vc.increment("node1")
        self.assertEqual(vc.clock["node1"], 2)
    
    def test_update(self):
        """Test clock merge/update."""
        vc1 = VectorClock({"A": 1, "B": 2})
        vc2 = VectorClock({"A": 2, "C": 1})
        
        vc1.update(vc2)
        self.assertEqual(vc1.clock["A"], 2)
        self.assertEqual(vc1.clock["B"], 2)
        self.assertEqual(vc1.clock["C"], 1)
    
    def test_compare_before(self):
        """Test causality: before relationship."""
        vc1 = VectorClock({"A": 1, "B": 1})
        vc2 = VectorClock({"A": 2, "B": 2})
        
        self.assertEqual(vc1.compare(vc2), "BEFORE")
        self.assertTrue(vc1.is_before(vc2))
    
    def test_compare_after(self):
        """Test causality: after relationship."""
        vc1 = VectorClock({"A": 2, "B": 2})
        vc2 = VectorClock({"A": 1, "B": 1})
        
        self.assertEqual(vc1.compare(vc2), "AFTER")
        self.assertTrue(vc1.is_after(vc2))
    
    def test_compare_concurrent(self):
        """Test conflict detection: concurrent writes."""
        vc1 = VectorClock({"A": 2, "B": 1})
        vc2 = VectorClock({"A": 1, "B": 2})
        
        self.assertEqual(vc1.compare(vc2), "CONCURRENT")
        self.assertTrue(vc1.is_concurrent(vc2))
    
    def test_compare_equal(self):
        """Test equality."""
        vc1 = VectorClock({"A": 1, "B": 2})
        vc2 = VectorClock({"A": 1, "B": 2})
        
        self.assertEqual(vc1.compare(vc2), "EQUAL")
        self.assertEqual(vc1, vc2)
    
    def test_copy(self):
        """Test deep copy."""
        vc1 = VectorClock({"A": 1, "B": 2})
        vc2 = vc1.copy()
        
        vc2.increment("A")
        self.assertEqual(vc1.clock["A"], 1)  # Original unchanged
        self.assertEqual(vc2.clock["A"], 2)  # Copy modified


class TestVersionedValue(unittest.TestCase):
    """Test cases for VersionedValue class."""
    
    def test_creation(self):
        """Test versioned value creation."""
        vc = VectorClock({"A": 1})
        vv = VersionedValue("test_value", vc)
        
        self.assertEqual(vv.value, "test_value")
        self.assertEqual(vv.vector_clock, vc)
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        vc = VectorClock({"A": 1, "B": 2})
        vv = VersionedValue("test", vc)
        
        data = vv.to_dict()
        self.assertEqual(data["value"], "test")
        self.assertEqual(data["vector_clock"], {"A": 1, "B": 2})
    
    def test_from_dict(self):
        """Test creation from dictionary."""
        data = {
            "value": "test",
            "vector_clock": {"A": 1, "B": 2}
        }
        
        vv = VersionedValue.from_dict(data)
        self.assertEqual(vv.value, "test")
        self.assertEqual(vv.vector_clock.clock, {"A": 1, "B": 2})


class TestReconciliation(unittest.TestCase):
    """Test cases for version reconciliation."""
    
    def test_no_conflict(self):
        """Test reconciliation with clear winner."""
        vc1 = VectorClock({"A": 1, "B": 1})
        vc2 = VectorClock({"A": 2, "B": 2})
        
        vv1 = VersionedValue("old", vc1)
        vv2 = VersionedValue("new", vc2)
        
        latest, conflicts = reconcile_versions([vv1, vv2])
        
        self.assertEqual(len(latest), 1)
        self.assertEqual(latest[0].value, "new")
        self.assertEqual(len(conflicts), 0)
    
    def test_with_conflict(self):
        """Test reconciliation with conflicts (siblings)."""
        vc1 = VectorClock({"A": 2, "B": 1})
        vc2 = VectorClock({"A": 1, "B": 2})
        
        vv1 = VersionedValue("version_A", vc1)
        vv2 = VersionedValue("version_B", vc2)
        
        latest, conflicts = reconcile_versions([vv1, vv2])
        
        self.assertEqual(len(latest), 2)
        self.assertEqual(len(conflicts), 2)
    
    def test_multiple_versions(self):
        """Test reconciliation with multiple versions."""
        vc1 = VectorClock({"A": 1})
        vc2 = VectorClock({"A": 2})
        vc3 = VectorClock({"A": 3})
        
        vv1 = VersionedValue("v1", vc1)
        vv2 = VersionedValue("v2", vc2)
        vv3 = VersionedValue("v3", vc3)
        
        latest, conflicts = reconcile_versions([vv1, vv2, vv3])
        
        self.assertEqual(len(latest), 1)
        self.assertEqual(latest[0].value, "v3")
        self.assertEqual(len(conflicts), 0)


def run_tests():
    """Run all tests."""
    print("\n" + "="*70)
    print("VECTOR CLOCK UNIT TESTS")
    print("="*70 + "\n")
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestVectorClock))
    suite.addTests(loader.loadTestsFromTestCase(TestVersionedValue))
    suite.addTests(loader.loadTestsFromTestCase(TestReconciliation))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.wasSuccessful():
        print("\n✓ ALL TESTS PASSED!")
        return 0
    else:
        print("\n✗ SOME TESTS FAILED")
        return 1


if __name__ == '__main__':
    exit(run_tests())
