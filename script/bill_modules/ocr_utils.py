import re
import numpy as np
import ast
class ocr_utils:
    def bb_convert_layout(l):
        """Remove text from bbox"""
        import re
        l=re.sub(r'[^0-9\.\ ]',"",str(l))
        return l.split()
    
    def dict_bb(a):
        """Convert bbox to dict"""
        if isinstance(a,str) and len(a)>0:
            a=ast.literal_eval(a)
        if 'nan' in str(a):
            return {} 

        if len(a) != 8:
            return {}
        # else:  
        keys = ['x1', 'y1', 'x2', 'y2', 'x3', 'y3', 'x4', 'y4']
        result = {}
        for i in range(8):
            result[keys[i]] = a[i]
        return result
    
    def row_wise_bb(df):
        """Calculate rowwise bb [x1, y1, x2, y2, x3, y3, x4, y4]"""
        bb_cols = [col for col in df.columns if col.startswith("bb") and col != "bb_rowwise"]
        for idx in df.index:
            bb_coords=[df.at[idx,col] for col in bb_cols if str(df.at[idx,col]) not in ["","nan"]]
            bb_coords=[ast.literal_eval(item) if isinstance(item,str) else item for item in bb_coords]
            bb_coords=[item for item in bb_coords if item!=[]]
 
            if len(bb_coords)>0:
                bb_coords=[bb_coords[0][0],bb_coords[0][1],bb_coords[0][6],bb_coords[0][7],bb_coords[-1][2],bb_coords[-1][3],bb_coords[-1][4],bb_coords[-1][5]]
                x1, y1, x4, y4, x2, y2, x3, y3 = bb_coords
                row_wise_bb = [x1, y1, x2, y2, x3, y3, x4, y4]
                df.at[idx, "bb_rowwise"] = str(row_wise_bb)

        return df

    def bb_convert_aws(d,height,width):
        """Convert x,y,w,h to x1, y1, x2, y2, x3, y3, x4, y4"""
        if isinstance(d,dict):
            #x1,y1
            x1,y1=d['Left']*width,d['Top']*height
            # x2,y2
            x2,y2=(d['Left']+d['Width'])*width,d['Top']*height
            #x3,y3
            x3,y3=(d['Left']+d['Width'])*width,(d['Top']+d['Height'])*height
            #x4,y4
            x4,y4=d['Left']*width,(d['Top']+d['Height'])*height
            result=[x1,y1,x2,y2,x3,y3,x4,y4]
            result=[round(item,2) for item in result]
            return result
        else:
            return ""



    def table_sorter(column_list):
        column_list=[i for i in column_list]
        """Sorting on invoice tables"""
        if "Quantity" in column_list and "Unit" in column_list and column_list.index("Quantity")>column_list.index("Unit"):
            column_list[column_list.index("Quantity")]="Unit1"
            column_list[column_list.index("Unit")]="Quantity"
            column_list[column_list.index("Unit1")]="Unit"
        
        desired_order = ['Date', 'Description','UnitPrice', 'Quantity','Unit', 'Amount']

        filtered_columns = [col for col in column_list if col in desired_order]
        

        sorted_columns = sorted(filtered_columns, key=lambda col: (desired_order.index(col), col))

        return sorted_columns
    def cleanup(df):
        df.columns=[str(i) for i in df.columns]
        for i in df.columns:
            df[i]=df[i].apply(lambda x:str(x).strip(":").rstrip("-").strip('.').strip())
        df.replace(":","",inplace=True)
        
        return df
