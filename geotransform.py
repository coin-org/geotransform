from pyproj import Transformer
from multiprocessing import Pool
import psycopg2


def update(si_nm):
    transformer = Transformer.from_crs('epsg:5179', 'epsg:4326', always_xy=True)  # UTM-K(Bassel) 도로명주소 지도 사용 중

    connection = psycopg2.connect(dbname='juso', user='postgres', password='password', host='happyjoy1234.asuscomm.com')
    connection2 = psycopg2.connect(dbname='juso', user='postgres', password='password',
                                   host='happyjoy1234.asuscomm.com')

    count = 0
    with connection2.cursor() as cursor2:
        with connection.cursor() as cursor:
            cursor.execute(
                "DECLARE super_cursor BINARY CURSOR FOR select * from address where si_nm = '" + si_nm + "'")
            while True:
                cursor.execute("FETCH 1000 FROM super_cursor")
                rows = cursor.fetchall()

                if not rows:
                    break

                for row in rows:
                    if row[16] != '' and row[17] != '':
                        long, lat = transformer.transform(row[16], row[17])
                        cursor2.execute("UPDATE address "
                                        "SET longitude = %s, latitude = %s "
                                        "WHERE dong_cd = %s and rn_mg_sn = %s and udrt_yn = %s and buld_mnnm = %s and buld_slno = %s"
                                        , (round(long, 6), round(lat, 6), row[2], row[6], row[8], row[9], row[10]))
                        count += 1

                print(si_nm + "-" + str(count))
                connection2.commit()
    return count


if __name__ == '__main__':
    with Pool(17) as p:
        print(p.map(update, [
            "대전광역시"
            , "인천광역시"
            , "울산광역시"
            , "광주광역시"
            , "강원도"
            , "부산광역시"
            , "대구광역시"
            , "서울특별시"
            , "전라남도"
            , "경기도"
            , "경상북도"
            , "제주특별자치도"
            , "전라북도"
            , "충청남도"
            , "경상남도"
            , "세종특별자치시"
            , "충청북도"
        ]))
