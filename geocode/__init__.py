try:
    #py2
    from geocode import Geocoder, query_yes_no
except:
    #py3+
    from geocode.geocode import Geocoder, query_yes_no

__all__ = ["Geocoder", "query_yes_no"]