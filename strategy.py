import time

def run_strategy(broker, config, logger):
    symbol = config["symbol"]
    qty = config["qty"]
    order_type = config["order_type"]
    product_type = config["product_type"]
    target_pnl = config["target_pnl"]
    stop_loss_pnl = config["stop_loss_pnl"]

    logger.info("Placing initial buy order...")
    order = broker.place_order(symbol, qty, 1, order_type, product_type)
    logger.info(f"Order Response: {order}")

    # Get order id if available
    order_id = order.get("id", None)

    while True:
        mtm = broker.get_mtm()
        logger.info(f"Current MTM: {mtm}")

        if mtm is None:
            logger.warning("Could not fetch MTM.")
            time.sleep(2)
            continue

        if mtm >= target_pnl:
            logger.info("Target reached. Exiting position.")
            broker.place_order(symbol, qty, -1, order_type, product_type)
            break
        elif mtm <= stop_loss_pnl:
            logger.info("Stop Loss hit. Exiting position.")
            broker.place_order(symbol, qty, -1, order_type, product_type)
            break

        time.sleep(2)
