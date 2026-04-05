import unittest
import requests
import time
import utils as tu

BASE_URL = "http://127.0.0.1:8000"
TIMEOUT = 15
POLL_INTERVAL = 0.2

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

def wait_for_order_state(order_id: str, expected_paid: bool, timeout=TIMEOUT):
    """Poll until order reaches expected paid state"""
    start = time.time()
    while time.time() - start < timeout:
        order = tu.find_order(order_id)
        if order["paid"] == expected_paid:
            return True
        time.sleep(POLL_INTERVAL)
    return False


class Test2PhaseCommit(unittest.TestCase):

    def test_2pc_success_case(self):
        """2PC commits successfully when all participants vote YES"""

        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item1 = tu.create_item(10)
        item_id1 = item1["item_id"]
        tu.add_stock(item_id1, 50)

        item2 = tu.create_item(15)
        item_id2 = item2["item_id"]
        tu.add_stock(item_id2, 30)

        order = tu.create_order(user_id)
        order_id = order["order_id"]

        tu.add_item_to_order(order_id, item_id1, 5)
        tu.add_item_to_order(order_id, item_id2, 3)

        response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_success(response.status_code))

        self.assertTrue(wait_for_order_state(order_id, True))

        self.assertEqual(tu.find_user(user_id)["credit"], 5)
        self.assertEqual(tu.find_item(item_id1)["stock"], 45)
        self.assertEqual(tu.find_item(item_id2)["stock"], 27)
        self.assertTrue(tu.find_order(order_id)["paid"])

    def test_2pc_stock_insufficient(self):
        """Abort when stock prepare fails"""

        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 2)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 5)

        initial_credit = tu.find_user(user_id)["credit"]
        initial_stock = tu.find_item(item_id)["stock"]

        response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(response.status_code))

        self.assertTrue(wait_for_order_state(order_id, False))

        self.assertEqual(tu.find_user(user_id)["credit"], initial_credit)
        self.assertEqual(tu.find_item(item_id)["stock"], initial_stock)
        self.assertFalse(tu.find_order(order_id)["paid"])

    def test_2pc_payment_insufficient(self):
        """Abort when payment prepare fails"""

        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 20)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 50)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 5)

        initial_credit = tu.find_user(user_id)["credit"]
        initial_stock = tu.find_item(item_id)["stock"]

        response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(response.status_code))

        self.assertTrue(wait_for_order_state(order_id, False))

        self.assertEqual(tu.find_user(user_id)["credit"], initial_credit)
        self.assertEqual(tu.find_item(item_id)["stock"], initial_stock)
        self.assertFalse(tu.find_order(order_id)["paid"])

    def test_2pc_atomicity_multiple_items(self):
        """All updates happen atomically across multiple items"""

        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 50)

        items = []
        for price in [5, 10, 15, 20]:
            item = tu.create_item(price)
            item_id = item["item_id"]
            tu.add_stock(item_id, 100)
            items.append((item_id, price))

        order = tu.create_order(user_id)
        order_id = order["order_id"]

        tu.add_item_to_order(order_id, items[0][0], 2)
        tu.add_item_to_order(order_id, items[1][0], 1)
        tu.add_item_to_order(order_id, items[2][0], 2)

        response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_success(response.status_code))

        self.assertTrue(wait_for_order_state(order_id, True))

        self.assertEqual(tu.find_item(items[0][0])["stock"], 98)
        self.assertEqual(tu.find_item(items[1][0])["stock"], 99)
        self.assertEqual(tu.find_item(items[2][0])["stock"], 98)
        self.assertEqual(tu.find_item(items[3][0])["stock"], 100)

        self.assertEqual(tu.find_user(user_id)["credit"], 0)
        self.assertTrue(tu.find_order(order_id)["paid"])

    def test_2pc_idempotency(self):
        """Same transaction should not execute twice"""

        user = tu.create_user()
        user_id = user["user_id"]
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item["item_id"]
        tu.add_stock(item_id, 10)

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        tu.add_item_to_order(order_id, item_id, 5)

        # First call
        response1 = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_success(response1.status_code))

        self.assertTrue(wait_for_order_state(order_id, True))

        # Second call (should not change anything)
        response2 = tu.checkout_order(order_id)

        self.assertEqual(tu.find_user(user_id)["credit"], 50)
        self.assertEqual(tu.find_item(item_id)["stock"], 5)
        self.assertTrue(tu.find_order(order_id)["paid"])
