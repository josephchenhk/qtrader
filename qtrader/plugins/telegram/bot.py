# -*- coding: utf-8 -*-
# @Time    : 3/1/2022 5:55 pm
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: bot.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

from datetime import datetime

from telegram import Update
from telegram.ext import Updater
from telegram.ext import ExtBot
from telegram.ext import CallbackContext
from telegram.ext import CommandHandler
from telegram.ext import MessageHandler
from telegram.ext import Filters

from qtrader_config import TELEGRAM_TOKEN
from qtrader_config import TELEGRAM_CHAT_ID


class CustomedExtBot(ExtBot):
    __slots__ = ('qtrader_status',)


def echo(update: Update, context: CallbackContext):
    """
    Echo command
    """
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="[ECHO] " + update.message.text
    )


def stop(update: Update, context: CallbackContext):
    """
    /stop [no parameters]: Stop the engine
    """
    context.bot.qtrader_status = "Terminated"
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"[{datetime.now()}] Terminating QTrader..."
    )


def balance(update: Update, context: CallbackContext):
    """
    /balance [no parameters]: Fetch balance
    """
    context.bot.get_balance = True
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"[{datetime.now()}] Fetching balance..."
    )


def positions(update: Update, context: CallbackContext):
    """
    /positions [no parameters]: Fetch positions of all gateways
    """
    context.bot.get_positions = True
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"[{datetime.now()}] Fetching positions..."
    )


def orders(update: Update, context: CallbackContext):
    """
    /orders [parameters]: Fetch orders
    - filter:str (optional) if "-a", only return alive orders
    - n:int (optional) number of displayed records
    """
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"[{datetime.now()}] {context.args}..."
    )
    if len(context.args) > 2:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=__doc__
        )
        return
    if len(context.args) == 2:
        if "-a" not in context.args:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=__doc__
            )
            return
        p = [arg for arg in context.args if arg != "-a"][0]
        try:
            num_orders_displayed = int(p)
        except ValueError:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=__doc__
            )
            return
        context.bot.active_orders = True
        context.bot.num_orders_displayed = num_orders_displayed
    if len(context.args) == 1:
        if context.args[0] == "-a":
            context.bot.active_orders = True
        else:
            try:
                num_orders_displayed = int(context.args[0])
            except ValueError:
                context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=__doc__
                )
                return
            context.bot.num_orders_displayed = num_orders_displayed

    context.bot.get_orders = True
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(f"[{datetime.now()}] Fetching orders (active_orders="
              f"{context.bot.active_orders}, num_orders_displayed="
              f"{context.bot.num_orders_displayed})..."))


def deals(update: Update, context: CallbackContext):
    """
    /deals [parameters]: fetch done deals
    - n:int (optional) number of displayed records
    """
    if len(context.args) > 1:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=__doc__
        )
        return
    elif len(context.args) == 1:
        try:
            num_deals_displayed = int(context.args[0])
        except ValueError:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=__doc__
            )
            return
        context.bot.num_deals_displayed = num_deals_displayed

    context.bot.get_deals = True
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(f"[{datetime.now()}] Fetching deals (num_deals_displayed="
              f"{context.bot.num_deals_displayed})...")
    )


def cancel_orders(update: Update, context: CallbackContext):
    """
    /cancel_orders [no parameters]: cancel all active orders
    """
    context.bot.cancel_orders = True
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"[{datetime.now()}] Canceling all active orders..."
    )


def cancel_order(update: Update, context: CallbackContext):
    """
    /cancel_order [parameters]: cancel an active order
    - order_id: str, the id of the order
    - gateway_name: str, the name of the gateway
    """
    if len(context.args) != 2:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=__doc__
        )
        return
    context.bot.cancel_order = True
    context.bot.cancel_order_id = str(context.args[0])
    context.bot.gateway_name = str(context.args[1])
    context.bot.send_message(
        chat_id=update.effective_chat.id, text=(
            f"[{datetime.now()}] Canceling order: order_id={context.args[0]},"
            f"gateway_name={context.args[1]}..."))


def send_order(update: Update, context: CallbackContext):
    """
    /send_order [parameters] send an order with string instruction:
    - order_string: str, security_code, quantity, direction(l/s), offset(o/c),
                    order_type(m/l/s), gateway_name, price(None),
                    stop_price(None)
    """
    if len(context.args) != 1 or len(context.args[0].split(",")) != 8:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=__doc__
        )
        return
    context.bot.send_order = True
    context.bot.order_string = context.args[0]
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"[{datetime.now()}] Send order ({context.args})..."
    )


def close_positions(update: Update, context: CallbackContext):
    """
    /close_positions [parameters]:
    - gateway_name: str, the name of the gateway
    """
    if len(context.args) > 1:
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=__doc__
        )
        return
    elif len(context.args) == 1:
        context.bot.close_positions_gateway_name = context.args[0]
    context.bot.close_positions = True
    context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"[{datetime.now()}] Closing positions..."
    )


class TelegramBot:
    """
    <QTrader TelegramBot> Available commands:"
    1. /stop
    2. /balance
    3. /positions
    4. /orders
    5. /deals
    6. /cancel_order
    7. /cancel_orders
    8. /send_order
    9. /close_positions
    10. /help
    """

    def __init__(self, token: str):
        # Handle responses (make updater.bot subclass, so that we can add
        # attributes to it)
        self.updater = Updater(
            use_context=True,
            bot=CustomedExtBot(
                token=token))
        self.updater.bot.qtrader_status = "Running"
        self.updater.bot.get_balance = False
        self.updater.bot.get_positions = False
        self.updater.bot.get_orders = False
        self.updater.bot.active_orders = False
        self.updater.bot.num_orders_displayed = 1
        self.updater.bot.get_deals = False
        self.updater.bot.num_deals_displayed = 1
        self.updater.bot.cancel_order = False
        self.updater.bot.cancel_order_id = None
        self.updater.bot.gateway_name = None
        self.updater.bot.cancel_orders = False
        self.updater.bot.send_order = False
        self.updater.bot.order_string = ""
        self.updater.bot.close_positions = False
        self.updater.bot.close_positions_gateway_name = None

        dispatcher = self.updater.dispatcher
        stop_handler = CommandHandler('stop', stop)
        balance_handler = CommandHandler('balance', balance)
        positions_handler = CommandHandler('positions', positions)
        orders_handler = CommandHandler('orders', orders)
        deals_handler = CommandHandler('deals', deals)
        cancel_order_handler = CommandHandler('cancel_order', cancel_order)
        cancel_orders_handler = CommandHandler('cancel_orders', cancel_orders)
        send_order_handler = CommandHandler('send_order', send_order)
        close_positions_handler = CommandHandler(
            'close_positions', close_positions)
        echo_handler = MessageHandler(Filters.text & (~Filters.command), echo)
        dispatcher.add_handler(echo_handler)
        dispatcher.add_handler(stop_handler)
        dispatcher.add_handler(balance_handler)
        dispatcher.add_handler(positions_handler)
        dispatcher.add_handler(orders_handler)
        dispatcher.add_handler(deals_handler)
        dispatcher.add_handler(cancel_order_handler)
        dispatcher.add_handler(cancel_orders_handler)
        dispatcher.add_handler(send_order_handler)
        dispatcher.add_handler(close_positions_handler)

        self.updater.start_polling()
        # self.updater.idle()

    @property
    def qtrader_status(self):
        return self.updater.bot.qtrader_status

    @qtrader_status.setter
    def qtrader_status(self, val: str):
        self.updater.bot.qtrader_status = val

    @property
    def get_balance(self):
        return self.updater.bot.get_balance

    @get_balance.setter
    def get_balance(self, val: bool):
        self.updater.bot.get_balance = val

    @property
    def get_positions(self):
        return self.updater.bot.get_positions

    @get_positions.setter
    def get_positions(self, val: bool):
        self.updater.bot.get_positions = val

    @property
    def get_orders(self):
        return self.updater.bot.get_orders

    @get_orders.setter
    def get_orders(self, val: bool):
        self.updater.bot.get_orders = val

    @property
    def num_orders_displayed(self):
        return self.updater.bot.num_orders_displayed

    @num_orders_displayed.setter
    def num_orders_displayed(self, val: bool):
        self.updater.bot.num_orders_displayed = val

    @property
    def active_orders(self):
        return self.updater.bot.active_orders

    @active_orders.setter
    def active_orders(self, val: bool):
        self.updater.bot.active_orders = val

    @property
    def get_deals(self):
        return self.updater.bot.get_deals

    @get_deals.setter
    def get_deals(self, val: bool):
        self.updater.bot.get_deals = val

    @property
    def num_deals_displayed(self):
        return self.updater.bot.num_deals_displayed

    @num_deals_displayed.setter
    def num_deals_displayed(self, val: bool):
        self.updater.bot.num_deals_displayed = val

    @property
    def cancel_order(self):
        return self.updater.bot.cancel_order

    @cancel_order.setter
    def cancel_order(self, val: bool):
        self.updater.bot.cancel_order = val

    @property
    def cancel_order_id(self):
        return self.updater.bot.cancel_order_id

    @cancel_order_id.setter
    def cancel_order_id(self, val: bool):
        self.updater.bot.cancel_order_id = val

    @property
    def gateway_name(self):
        return self.updater.bot.gateway_name

    @gateway_name.setter
    def gateway_name(self, val: bool):
        self.updater.bot.gateway_name = val

    @property
    def cancel_orders(self):
        return self.updater.bot.cancel_orders

    @cancel_orders.setter
    def cancel_orders(self, val: bool):
        self.updater.bot.cancel_orders = val

    @property
    def send_order(self):
        return self.updater.bot.send_order

    @send_order.setter
    def send_order(self, val: bool):
        self.updater.bot.send_order = val

    @property
    def order_string(self):
        return self.updater.bot.order_string

    @order_string.setter
    def order_string(self, val: bool):
        self.updater.bot.order_string = val

    @property
    def close_positions(self):
        return self.updater.bot.close_positions

    @close_positions.setter
    def close_positions(self, val: bool):
        self.updater.bot.close_positions = val

    @property
    def close_positions_gateway_name(self):
        return self.updater.bot.close_positions_gateway_name

    @close_positions_gateway_name.setter
    def close_positions_gateway_name(self, val: bool):
        self.updater.bot.close_positions_gateway_name = val

    def close(self):
        self.updater.stop()

    def get_updates(self):
        self.updates = self.updater.bot.get_updates()
        print(self.updates[0])

    def get_chat_id(self):
        self.get_updates()
        chat_id = self.updates[0].message.from_user.id
        return chat_id

    def send_message(self, msg: str, chat_id: int = TELEGRAM_CHAT_ID):
        # chat_id = updates[0].message.from_user.id
        self.updater.bot.send_message(
            chat_id=chat_id,
            text=msg
        )


bot = TelegramBot(token=TELEGRAM_TOKEN)

if __name__ == "__main__":
    if "bot" not in locals():
        bot = TelegramBot(token=TELEGRAM_TOKEN)

    # before get_updates, send a msg to bot to initiate chat
    # bot.get_updates()
    # chat_id = bot.updates[0].message.from_user.id

    chat_id = TELEGRAM_CHAT_ID
    bot.send_message(chat_id=chat_id, msg="Yes I am here!")

    bot.close()
    print("Closed.")
