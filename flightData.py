from amadeus import Client, ResponseError

amadeus = Client(
    client_id='is5rFnQvR4pPC28WSgaAuCz9YzYeCYiW',
    client_secret='L6a5V9hTFciC5RhG'
)
try:
    response = amadeus.shopping.flight_offers_search.get(
        originLocationCode='SYD',
        destinationLocationCode='YYZ',
        departureDate='2020-10-01',
        adults=1)
    for i in range( 0, len(response.data[0]['itineraries'][0]["segments"])):
        print(response.data[0]['itineraries'][0]["segments"][0])
        print(response.data[0]['itineraries'][0]["segments"][1])
except ResponseError as error:
    print(error)
    