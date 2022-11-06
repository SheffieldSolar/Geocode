try:
    #py2
    from geocode import Geocoder
except:
    #py3+
    from geocode.geocode import Geocoder

__all__ = ["Geocoder"]