import unittest

# Run with command:
# python run_tests.py

if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    suite.addTests(loader.loadTestsFromName('microservices'))
    suite.addTests(loader.loadTestsFromName('credits'))
    suite.addTests(loader.loadTestsFromName('failure_resistance'))
    suite.addTests(loader.loadTestsFromName('concurrent_payments'))
    suite.addTests(loader.loadTestsFromName('coordinator'))
    suite.addTests(loader.loadTestsFromName('rollback'))

    suite.addTests(loader.loadTestsFromName('kafka_saga'))

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
