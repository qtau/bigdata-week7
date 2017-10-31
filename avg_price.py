from mrjob.job import MRJob
from mrjob.step import MRStep    
     
class MRAvgPrice(MRJob):

    # map the client id with the price of one ticket
    def mapper(self, _, tree_line):
        info = tree_line.split()
        yield (info[0], info[1])

    # Get the quantity of tickets bought and compute the total price of all the tickets
    # Then compute the average
    def combiner(self, client, price_ticket):
        nb_tickets = 0
        total_price = 0
        for price in price_ticket:
            total_price += float(price)
            nb_tickets += 1
        yield (client, total_price/nb_tickets)

    def reducer(self, client, price_ticket):
        nb_tickets = 0
        total_price = 0
        for price in price_ticket:
            total_price += float(price)
            nb_tickets += 1
        yield (client, total_price/nb_tickets)

if __name__ == '__main__':
    MRAvgPrice.run()
