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

def sdoOrdinates_2_wkt (lob_dic):
    """Converts cxoracle LBO dict to wkt string - only polygons for now"""
    sdo_gtyp = lob_dic['SDO_GTYPE']
    
    if str(sdo_gtyp)[-2:] == '03':
        sdo_ord = lob_dic['SDO_ORDINATES']
        l_len = len(sdo_ord)
        l_paired = [str(sdo_ord[i]) + " " + str(sdo_ord[i+1]) for i in range(0, l_len-1, 2)]
        
        if l_len % 2 == 1:
            l_paired.append(sdo_ord[l_len-1])
        
        s_ord = ", ".join (x for x in l_paired)
        
        s_wkt = "POLYGON (({}))".format(s_ord) 
        
    else:
        print('This Geometry is not a Polygon.')
    
    return s_wkt
