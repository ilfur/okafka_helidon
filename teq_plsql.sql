grant AQ_USER_ROLE to teqdemo;
grant select_catalog_role to teqdemo;
grant AQ_ADMINISTRATOR_ROLE to teqdemo;
grant execute on sys.dbms_aq to teqdemo;
grant execute on sys.dbms_aqin to teqdemo;
grant execute on sys.dbms_aqjms to teqdemo;
alter user teqdemo quota unlimited on users;

begin
dbms_aqadm.grant_system_privilege(privilege =>
'DEQUEUE_ANY', grantee => 'TEQDEMO', admin_option =>
TRUE);
end;

begin
sys.dbms_aqadm.create_sharded_queue(queue_name=>'TEQ', multiple_consumers => TRUE); 
sys.dbms_aqadm.set_queue_parameter('TEQ', 'SHARD_NUM', 1);
sys.dbms_aqadm.set_queue_parameter('TEQ', 'STICKY_DEQUEUE', 1);
sys.dbms_aqadm.set_queue_parameter('TEQ', 'KEY_BASED_ENQUEUE', 1);
sys.dbms_aqadm.start_queue('TEQ');
end;

begin
sys.dbms_aqadm.stop_queue('TEQ');
sys.dbms_aqadm.drop_sharded_queue(queue_name=>'TEQ'); 
end;

select owner, object_name from dba_objects
where object_name like 'MESSAGE_TYPE';

show parameter streams
show parameter local
alter system set LOCAL_LISTENER='(ADDRESS=(PROTOCOL=TCP)(HOST=db21c )(PORT=1521))' 
alter system set streams_pool_size=64M scope=spfile;







begin
sys.dbms_aqadm.create_sharded_queue(queue_name=>'TEQ', multiple_consumers => TRUE); 
sys.dbms_aqadm.set_queue_parameter('TEQ', 'SHARD_NUM', 1);
sys.dbms_aqadm.set_queue_parameter('TEQ', 'STICKY_DEQUEUE', 1);
sys.dbms_aqadm.set_queue_parameter('TEQ', 'KEY_BASED_ENQUEUE', 0);
begin
sys.dbms_aqadm.start_queue('TEQ');
end;

declare
  pval number;
begin
  dbms_aqadm.get_queue_parameter('TEQ', 'AQ$GET_KEY_SHARD=100', pval);
  dbms_output.put_line('The key 100 is mapped to shard id ' || pval);
END;


DECLARE 

    text        varchar2(32767); 
    bytes       RAW(32767); 
    body        BLOB; 
    position    INT; 
    --agent       sys.aq$_agent   := sys.aq$_agent('consumers', null, 0); 
    message     sys.aq$_jms_bytes_message; 

    enqueue_options    dbms_aq.enqueue_options_t; 
    message_properties dbms_aq.message_properties_t; 
    msgid               raw(16); 

BEGIN 

    message := sys.aq$_jms_bytes_message.construct; 
    message_properties.correlation := '100';

    --message.set_replyto(agent); 
    --message.set_type('tkaqpet2'); 
    message.set_userid('teqdemo'); 
    message.set_appid('okafka'); 
    message.set_groupid('producer'); 
    message.set_groupseq(1); 
    message.set_string_property('topic','TEQ');
    message.set_string_property('AQINTERNAL_PARTITION','0');

    FOR i IN 1..500 LOOP 
        text := CONCAT (text, '1234567890'); 
    END LOOP; 

    --message.set_text(text); 
    bytes := HEXTORAW(text); 

    dbms_lob.createtemporary(lob_loc => body, cache => TRUE); 
    dbms_lob.open (body, DBMS_LOB.LOB_READWRITE); 
    position := 1 ; 
    FOR i IN 1..10 LOOP 
        dbms_lob.write ( lob_loc => body, 
                amount => FLOOR((LENGTH(bytes)+1)/2), 
                offset => position, 
                buffer => bytes); 
        position := position + FLOOR((LENGTH(bytes)+1)/2) ; 
    END LOOP; 
    
    message.set_bytes(body); 
    

    dbms_aq.enqueue(queue_name => 'TEQ', 
                       enqueue_options => enqueue_options, 
                       message_properties => message_properties, 
                       payload => message, 
                       msgid => msgid); 
                       
    dbms_lob.freetemporary(lob_loc => body); 

END; 





DECLARE

    id                 pls_integer;
    blob_data          blob;
    clob_data          clob;
    blob_len           pls_integer;
    message            sys.aq$_jms_bytes_message;
    agent              sys.aq$_agent;
    dequeue_options    dbms_aq.dequeue_options_t;
    message_properties dbms_aq.message_properties_t;
    msgid raw(16);
    gdata              sys.aq$_jms_value;

    java_exp           exception;
    pragma EXCEPTION_INIT(java_exp, -24197);
BEGIN
    DBMS_OUTPUT.ENABLE (20000);

    -- Dequeue this message from AQ queue using DBMS_AQ package
    dbms_aq.dequeue(queue_name => 'TEQ',
                    dequeue_options => dequeue_options,
                    message_properties => message_properties,
                    payload => message,
                    msgid => msgid);

    -- Retrieve the header 
    agent := message.get_replyto;

    dbms_output.put_line('Type: ' || message.get_type || 
                         ' UserId: ' || message.get_userid || 
                         ' AppId: ' || message.get_appid || 
                         ' GroupId: ' || message.get_groupid || 
                         ' GroupSeq: ' || message.get_groupseq);

    -- Retrieve the user properties
    dbms_output.put_line('price: ' || message.get_float_property('price'));
    dbms_output.put_line('color: ' || message.get_string_property('color'));
    IF message.get_boolean_property('import') = TRUE THEN
       dbms_output.put_line('import: Yes' );
    ELSIF message.get_boolean_property('import') = FALSE THEN
       dbms_output.put_line('import: No' );
    END IF;
    dbms_output.put_line('year: ' || message.get_int_property('year'));
    dbms_output.put_line('mileage: ' || message.get_long_property('mileage'));
    dbms_output.put_line('password: ' || message.get_byte_property('password'));

-- Shows how to retrieve the message payload of aq$_jms_bytes_message

-- Prepare call, send the content in the PL/SQL aq$_jms_bytes_message object to 
   -- Java stored procedure(Jserv) in the form of a byte array. 
   -- Passing -1 reserves a new slot in msg store of sys.aq$_jms_bytes_message.
   -- Max number of sys.aq$_jms_bytes_message type of messges to be operated at
   -- the same time in a session is 20. Call clean_body fn. with parameter -1 
   -- might result in ORA-24199 error if messages operated on are already 20. 
   -- You must call clean or clean_all function to clean up message store.
    id := message.prepare(-1);

-- Read data from BytesMessage paylaod. These fns. are analogy of JMS Java
-- API's. See the JMS Types chapter for detail.
    dbms_output.put_line('Payload:');

    -- read a byte from the BytesMessage payload
    dbms_output.put_line('read_byte:' || message.read_byte(id));

    -- read a byte array into a blob object from the BytesMessage payload
    dbms_output.put_line('read_bytes:');
    blob_len := message.read_bytes(id, blob_data, 272);
    display_blob(blob_data);

    -- read a char from the BytesMessage payload
    dbms_output.put_line('read_char:'|| message.read_char(id));

    -- read a double from the BytesMessage payload
    dbms_output.put_line('read_double:'|| message.read_double(id));

    -- read a float from the BytesMessage payload
    dbms_output.put_line('read_float:'|| message.read_float(id));

    -- read a int from the BytesMessage payload
    dbms_output.put_line('read_int:'|| message.read_int(id));

    -- read a long from the BytesMessage payload
    dbms_output.put_line('read_long:'|| message.read_long(id));

    -- read a short from the BytesMessage payload
    dbms_output.put_line('read_short:'|| message.read_short(id));

    -- read a String from the BytesMessage payload. 
    -- the String is in UTF8 encoding in the message payload
    dbms_output.put_line('read_utf:');
    message.read_utf(id, clob_data);
    display_clob(clob_data);

    -- Use either clean_all or clean to clean up the message store when the user 
    -- do not plan to do paylaod retrieving on this message anymore
    message.clean(id);
    -- sys.aq$_jms_bytes_message.clean_all();

    EXCEPTION 
    WHEN java_exp THEN
      dbms_output.put_line('exception information:');
      display_exp(sys.aq$_jms_bytes_message.get_exception());

END;


