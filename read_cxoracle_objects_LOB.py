def ObjectRepr(obj):
    """Reads cx_Oracle LOB and turns it into a dictionary"""
    if obj.type.iscollection:
        returnValue = []
        for value in obj.aslist():
            if isinstance(value, cx_Oracle.Object):
                value = ObjectRepr(value)
            returnValue.append(value)
    else:
        returnValue = {}
        for attr in obj.type.attributes:
            value = getattr(obj, attr.name)
            if value is None:
                continue
            elif isinstance(value, cx_Oracle.Object):
                value = ObjectRepr(value)
            returnValue[attr.name] = value
      
    return returnValue
