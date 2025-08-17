from abc import ABC, abstractmethod

class BrokerInterface(ABC):
    @abstractmethod
    def place_order(self, symbol, qty, side, order_type, product_type, price=0):
        pass

    @abstractmethod
    def get_positions(self):
        pass

    @abstractmethod
    def get_order_status(self, order_id):
        pass

    @abstractmethod
    def get_mtm(self):
        pass

    @abstractmethod
    def cancel_order(self, order_id):
        pass
