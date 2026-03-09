import unittest
import subprocess
import requests
import time
import os

import utils as tu

COMPOSE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

class TestMicroservices(unittest.TestCase):

    def test_stock(self):
        # Test /stock/item/create/<price>
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        # Test /stock/find/<item_id>
        item: dict = tu.find_item(item_id)
        self.assertEqual(item['price'], 5)
        self.assertEqual(item['stock'], 0)

        # Test /stock/add/<item_id>/<number>
        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(200 <= int(add_stock_response) < 300)

        stock_after_add: int = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_add, 50)

        # Test /stock/subtract/<item_id>/<number>
        over_subtract_stock_response = tu.subtract_stock(item_id, 200)
        self.assertTrue(tu.status_code_is_failure(int(over_subtract_stock_response)))

        subtract_stock_response = tu.subtract_stock(item_id, 15)
        self.assertTrue(tu.status_code_is_success(int(subtract_stock_response)))

        stock_after_subtract: int = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_subtract, 35)

    def test_payment(self):
        # Test /payment/pay/<user_id>/<order_id>
        user: dict = tu.create_user()
        self.assertIn('user_id', user)

        user_id: str = user['user_id']

        # Test /users/credit/add/<user_id>/<amount>
        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(add_credit_response))

        # add item to the stock service
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        # create order in the order service and add item to the order
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)

        order_id: str = order['order_id']

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        payment_response = tu.payment_pay(user_id, 10)
        self.assertTrue(tu.status_code_is_success(payment_response))

        credit_after_payment: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after_payment, 5)

    def test_order(self):
        # Test /payment/pay/<user_id>/<order_id>
        user: dict = tu.create_user()
        self.assertIn('user_id', user)

        user_id: str = user['user_id']

        # create order in the order service and add item to the order
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)

        order_id: str = order['order_id']

        # add item to the stock service
        item1: dict = tu.create_item(5)
        self.assertIn('item_id', item1)
        item_id1: str = item1['item_id']
        add_stock_response = tu.add_stock(item_id1, 15)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        # add item to the stock service
        item2: dict = tu.create_item(5)
        self.assertIn('item_id', item2)
        item_id2: str = item2['item_id']
        add_stock_response = tu.add_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        add_item_response = tu.add_item_to_order(order_id, item_id1, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        add_item_response = tu.add_item_to_order(order_id, item_id2, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        subtract_stock_response = tu.subtract_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(subtract_stock_response))

        checkout_response = tu.checkout_order(order_id).status_code
        self.assertTrue(tu.status_code_is_failure(checkout_response))

        stock_after_subtract: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_after_subtract, 15)

        add_stock_response = tu.add_stock(item_id2, 15)
        self.assertTrue(tu.status_code_is_success(int(add_stock_response)))

        credit_after_payment: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after_payment, 0)

        checkout_response = tu.checkout_order(order_id).status_code
        self.assertTrue(tu.status_code_is_failure(checkout_response))

        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(int(add_credit_response)))

        credit: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 15)

        stock: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock, 15)

        checkout_response = tu.checkout_order(order_id)
        print(checkout_response.text)
        self.assertTrue(tu.status_code_is_success(checkout_response.status_code))

        stock_after_subtract: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_after_subtract, 14)

        credit: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 5)

class TestFailureResilience(unittest.TestCase):

    def _stop_service(self, service: str):
        subprocess.run(
            ["docker", "compose", "stop", service], 
            check=True
        )
        time.sleep(2)

    def _start_service(self, service: str):
        subprocess.run(
            ["docker", "compose", "start", service], 
            check=True
        )
        time.sleep(3)

    def test_stock_service_down(self):
        # Create user
        user = tu.create_user()
        # Add credit to user
        tu.add_credit_to_user(user['user_id'], 100)
        # Create order for the user
        order = tu.create_order(user['user_id'])

        # Try to checkout while service is down
        self._stop_service("stock-service")
        try:
            response = tu.checkout_order(order['order_id'])
            # Must not be a 500, should be a clean 400
            self.assertNotEqual(response.status_code, 500)
            self.assertTrue(tu.status_code_is_failure(response.status_code))

            # Credit must be the same
            credit = tu.find_user(user['user_id'])['credit']
            self.assertEqual(credit, 100)
        finally:
            # Start back the stock service
            self._start_service("stock-service")

    def test_payment_service_down(self):
        # Create the item
        item = tu.create_item(5)
        # Add stocks to the item
        tu.add_stock(item['item_id'], 50)
        # Create a user
        user = tu.create_user()
        # Create order for the user
        order = tu.create_order(user['user_id'])
        # Add the item to the order
        tu.add_item_to_order(order['order_id'], item['item_id'], 1)

        stock_before = tu.find_item(item['item_id'])['stock']

        self._stop_service("payment-service")
        try:
            response = tu.checkout_order(order['order_id'])
            self.assertTrue(tu.status_code_is_failure(response.status_code))

            # Stock should be rolled back
            stock_after = tu.find_item(item['item_id'])['stock']
            self.assertEqual(stock_after, stock_before)
        finally:
            # Start back the payment service
            self._start_service("payment-service")

    def test_stock_service_recovers(self):
        # Restart stock service
        self._stop_service("stock-service")
        self._start_service("stock-service")

        # Should work after recovery
        item = tu.create_item(5)
        tu.add_stock(item['item_id'], 10)
        stock = tu.find_item(item['item_id'])['stock']
        self.assertEqual(stock, 10)

 class Test2PhaseCommit(unittest.TestCase):
    def test_2pc_success_case(self):
        """Test that 2PC successfully commits when all resources are available"""
        # Create user with sufficient credit
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)
        
        # Create items with sufficient stock
        item1: dict = tu.create_item(10)
        item_id1: str = item1['item_id']
        tu.add_stock(item_id1, 50)
        
        item2: dict = tu.create_item(15)
        item_id2: str = item2['item_id']
        tu.add_stock(item_id2, 30)
        
        # Create order and add items
        order: dict = tu.create_order(user_id)
        order_id: str = order['order_id']
        tu.add_item_to_order(order_id, item_id1, 5)
        tu.add_item_to_order(order_id, item_id2, 3)
        
        # Verify initial state
        initial_stock1 = tu.find_item(item_id1)['stock']
        initial_stock2 = tu.find_item(item_id2)['stock']
        initial_credit = tu.find_user(user_id)['credit']
        
        self.assertEqual(initial_stock1, 50)
        self.assertEqual(initial_stock2, 30)
        self.assertEqual(initial_credit, 100)
        
        # Checkout with 2PC (total cost: 5*10 + 3*15 = 95)
        checkout_response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_success(checkout_response.status_code))
        
        # Verify all changes committed atomically
        final_stock1 = tu.find_item(item_id1)['stock']
        final_stock2 = tu.find_item(item_id2)['stock']
        final_credit = tu.find_user(user_id)['credit']
        
        self.assertEqual(final_stock1, 45)  # 50 - 5
        self.assertEqual(final_stock2, 27)  # 30 - 3
        self.assertEqual(final_credit, 5)   # 100 - 95
        
        # Verify order is marked as paid
        order_status = tu.find_order(order_id)
        self.assertTrue(order_status['paid'])

    def test_2pc_stock_insufficient_prepare_fails(self):
        """Test that 2PC properly aborts when stock is insufficient during prepare phase"""
        # Create user with sufficient credit
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)
        
        # Create items - one with insufficient stock
        item1: dict = tu.create_item(10)
        item_id1: str = item1['item_id']
        tu.add_stock(item_id1, 50)
        
        item2: dict = tu.create_item(15)
        item_id2: str = item2['item_id']
        tu.add_stock(item_id2, 2)  # Only 2 in stock, but order needs 5
        
        # Create order and add items
        order: dict = tu.create_order(user_id)
        order_id: str = order['order_id']
        tu.add_item_to_order(order_id, item_id1, 5)
        tu.add_item_to_order(order_id, item_id2, 5)  # Request more than available
        
        # Verify initial state
        initial_stock1 = tu.find_item(item_id1)['stock']
        initial_stock2 = tu.find_item(item_id2)['stock']
        initial_credit = tu.find_user(user_id)['credit']
        
        # Checkout should fail
        checkout_response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(checkout_response.status_code))
        
        # Verify NO changes were made (atomicity)
        final_stock1 = tu.find_item(item_id1)['stock']
        final_stock2 = tu.find_item(item_id2)['stock']
        final_credit = tu.find_user(user_id)['credit']
        
        self.assertEqual(final_stock1, initial_stock1)
        self.assertEqual(final_stock2, initial_stock2)
        self.assertEqual(final_credit, initial_credit)
        
        # Verify order is NOT marked as paid
        order_status = tu.find_order(order_id)
        self.assertFalse(order_status['paid'])

    def test_2pc_payment_insufficient_prepare_fails(self):
        """Test that 2PC properly aborts when payment is insufficient during prepare phase"""
        # Create user with insufficient credit
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 20)  # Only 20 credit
        
        # Create items with sufficient stock
        item1: dict = tu.create_item(10)
        item_id1: str = item1['item_id']
        tu.add_stock(item_id1, 50)
        
        item2: dict = tu.create_item(15)
        item_id2: str = item2['item_id']
        tu.add_stock(item_id2, 30)
        
        # Create order and add items (total cost: 5*10 + 3*15 = 95, but user only has 20)
        order: dict = tu.create_order(user_id)
        order_id: str = order['order_id']
        tu.add_item_to_order(order_id, item_id1, 5)
        tu.add_item_to_order(order_id, item_id2, 3)
        
        # Verify initial state
        initial_stock1 = tu.find_item(item_id1)['stock']
        initial_stock2 = tu.find_item(item_id2)['stock']
        initial_credit = tu.find_user(user_id)['credit']
        
        # Checkout should fail
        checkout_response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(checkout_response.status_code))
        
        # Verify NO changes were made (atomicity - stock should be rolled back)
        final_stock1 = tu.find_item(item_id1)['stock']
        final_stock2 = tu.find_item(item_id2)['stock']
        final_credit = tu.find_user(user_id)['credit']
        
        self.assertEqual(final_stock1, initial_stock1)
        self.assertEqual(final_stock2, initial_stock2)
        self.assertEqual(final_credit, initial_credit)
        
        # Verify order is NOT marked as paid
        order_status = tu.find_order(order_id)
        self.assertFalse(order_status['paid'])

    def test_2pc_atomicity_multiple_items(self):
        """Test that 2PC maintains atomicity with multiple items"""
        # Create user with exact amount needed
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 50)
        
        # Create multiple items
        items = []
        for price in [5, 10, 15, 20]:
            item = tu.create_item(price)
            item_id = item['item_id']
            tu.add_stock(item_id, 100)
            items.append((item_id, price))
        
        # Create order with items totaling exactly 50
        order: dict = tu.create_order(user_id)
        order_id: str = order['order_id']
        tu.add_item_to_order(order_id, items[0][0], 2)  # 2 * 5 = 10
        tu.add_item_to_order(order_id, items[1][0], 1)  # 1 * 10 = 10
        tu.add_item_to_order(order_id, items[2][0], 2)  # 2 * 15 = 30
        # Total: 50
        
        # Checkout should succeed
        checkout_response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_success(checkout_response.status_code))
        
        # Verify all stock updated correctly
        self.assertEqual(tu.find_item(items[0][0])['stock'], 98)
        self.assertEqual(tu.find_item(items[1][0])['stock'], 99)
        self.assertEqual(tu.find_item(items[2][0])['stock'], 98)
        self.assertEqual(tu.find_item(items[3][0])['stock'], 100)  # Unchanged
        
        # Verify credit is now 0
        self.assertEqual(tu.find_user(user_id)['credit'], 0)
        
        # Verify order is paid
        self.assertTrue(tu.find_order(order_id)['paid'])

class TestCoordinatorFailure(unittest.TestCase):

    def _kill_order_service(self):
        subprocess.run(
            ["docker", "compose", "stop", "order-service"],
            cwd=COMPOSE_DIR, check=True
        )
        time.sleep(2)

    def _start_order_service(self):
        subprocess.run(
            ["docker", "compose", "start", "order-service"],
            cwd=COMPOSE_DIR, check=True
        )
        time.sleep(5)  

    def test_crash_before_commit_point(self):
        # Create item
        item = tu.create_item(5)
        # Add stocks to item
        tu.add_stock(item['item_id'], 50)
        # Create user
        user = tu.create_user()
        # Add credit to user
        tu.add_credit_to_user(user['user_id'], 100)
        # Create order for user
        order = tu.create_order(user['user_id'])
        # Add item to order
        tu.add_item_to_order(order['order_id'], item['item_id'], 1)

        stock_before = tu.find_item(item['item_id'])['stock']
        credit_before = tu.find_user(user['user_id'])['credit']

        # Simulate crash by killing order service
        self._kill_order_service()
        self._start_order_service()

        # After recovery state must be unchanged
        stock_after = tu.find_item(item['item_id'])['stock']
        credit_after = tu.find_user(user['user_id'])['credit']

        self.assertEqual(stock_after, stock_before,
            "Stock changed after coordinator crash before commit point")
        self.assertEqual(credit_after, credit_before,
            "Credit changed after coordinator crash before commit point")

    def test_system_works_after_coordinator_recovery(self):
        self._kill_order_service()
        self._start_order_service()

        # Create item
        item = tu.create_item(5)
        # Add stocks to item
        tu.add_stock(item['item_id'], 20)
        # Create user
        user = tu.create_user()
        # Add credit to user
        tu.add_credit_to_user(user['user_id'], 100)
        # Create an order
        order = tu.create_order(user['user_id'])
        # Add an item to the order
        tu.add_item_to_order(order['order_id'], item['item_id'], 1)

        # Checkout the order
        response = tu.checkout_order(order['order_id'])
        self.assertTrue(
            tu.status_code_is_success(response.status_code),
            f"Checkout failed after coordinator recovery: {response.text}"
        )

        self.assertEqual(tu.find_item(item['item_id'])['stock'], 19)
        self.assertEqual(tu.find_user(user['user_id'])['credit'], 95)

if __name__ == '__main__':
    unittest.main()
