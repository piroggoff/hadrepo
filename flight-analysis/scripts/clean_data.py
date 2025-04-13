import cleaning


print("1. Сохранение данных в raw\n2. экспорт данных в HBase\n3. просмотр результата\n")
mode = int(input())

df = cleaning.start_and_read()

df_cleaned = cleaning.clean_data(df)

if mode == 1:
    print("1. сохраняить партициями(фрагментированно)\n2. сохранить одним файлом(неэфффективно)\n")
    mode = int(input())

    df_processed = cleaning.process_segments(df_cleaned)
    cleaning.save_to_data(df_processed,mode)

    if mode not in [1,2]: print('blya')


elif mode == 2:
    hbase_df=cleaning.prepare_hbase_data(df)


elif mode == 3:
    print("1. Просмотр processed data\n2. show hbase prepare data\n")
    mode = int(input())
    if mode == 1:
        df_processed = cleaning.process_segments(df_cleaned)
        df_processed.show(10, truncate=False)
    elif mode == 2:
        df_processed = cleaning.process_segments(df_cleaned)
        hbase_df=cleaning.prepare_hbase_data(df_processed)
        hbase_df.show(10, truncate=False)

    else: print('suck')

else: print("бля")


