import unittest
import subprocess
import requests
import time
import os
import threading

import utils as tu

COMPOSE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

PAYMENT_URL = "http://127.0.0.1:8000"

ROLLBACK_TIMEOUT = 15 # seconds to wait for async

#                              GGCCLffCGLfttfffft    
#                          LfffffLCGGLfLCCC008880GLft
#                         CLffttttiii11iitfLLfCG8@@8Gft 
#                       tCCLLft11i::;;;ii;i1Lft1tLCG88Gf
#                   i1fLCCfti:,,::::;;i1111i1fLfi1fLL0@8Gtt 
#                  tLG0GLti:,,,::::::;ii1ii1ii1fftffCG08@0f1
#                1C00C   ,,,,,,:;;;;;ii111iiiiii1tftLGG88881: 
#                       ...    .,:;ii11tttt1iiiiiii1fLCC08801,
#                      ....      .,:;;;;;i11iiiiiii:ifLCC08881,,
#                       ......    .,:,..  .,,:;;;ii;,iCLf1G@881,,
#                      ,.....,,.  .:;:...,,,,,:,:ii;::1LtitC8@G;,
#                      ,,..,.,::,.:ii;, ...,::;::;i;:::;;;iiL8@C:,
#                       ,,:::::,,:i1iii:,...,,:::;;;::,:,,::iC8Cii,
#      2.P.a.C.         .,,::,,,,;1tiii1i;;;ii;:;;;;:;;:::;::
#      tests            :..,..,::itt1ii;;;;;i;;:;;;:;;i,.,:::
#                       ;,..  ..:;i;;;i;;::,:::::::::;:,,,,
#                        .       ....,::::::::,:::::,,,,,, 
#                       ..     ..,,:::,,  ,:::,:::::,
#                       ..   ..,,:;;;;;;,..,:,::::::,
#                        .  ..:;i11tti;;:,,,,,::::i111ii11ii
#                        .    ..,,::::::,,,,,,,::;i1ii111111ii
#                                .,,:::,,,,,,,:;;i1tiiiiii11111i 
#                       ,.     ..,,,:,....,,:;;iii1t1;,,.,:;;;;11ii
#                      i;,            ..,::;;;ii1tLLCfi:::;;;:;11ii
#               iiiiii1;..          ..,,:::;;i1tf00G0Gfi;;;i1i;11i 
#            ii11i111i; ..        ..,,:::;1tLLCfLLfti;;iiii1tt11iii
#        i1tt1i;;i1ii;, .    .....,,::;1tLCLLft11;::;11111ttttfft1i
#    ii1iii1;::iii;;;: .:   .,,,,,:;;1ttt111i;;::::;;i1tftttftttttt1 
#   ;;;;;;;::;ii;;:::..ii...:::,:;i1i;;;;::::::,::::::;i11ttttttffttt
#;:;;::;;;:;ii;:;::;: ;tt::::::;;;;;::::,,,::,,::,,,::,,:;ii1ttttttt

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


