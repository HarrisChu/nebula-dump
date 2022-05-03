package pkg

import "github.com/facebook/fbthrift/thrift/lib/go/thrift"

func deserialize(pf thrift.ProtocolFactory, data *[]byte, s thrift.Struct) error {
	transport := thrift.NewMemoryBufferWithData(*data)
	protocol := pf.GetProtocol(transport)
	err := s.Read(protocol)
	if err != nil {
		return err
	}
	return nil
}

func serialize(pf thrift.ProtocolFactory, data *[]byte, s thrift.Struct) error {
	transport := thrift.NewMemoryBuffer()
	protocol := pf.GetProtocol(transport)
	err := s.Write(protocol)
	if err != nil {
		return err
	}
	*data = make([]byte, len(transport.Bytes()))
	copy(*data, transport.Bytes())
	return nil
}

func CompactSerializer(s thrift.Struct, data *[]byte) error {
	pf := thrift.NewCompactProtocolFactory()
	return serialize(pf, data, s)
}

func CompactDeserializer(s thrift.Struct, data *[]byte) error {
	pf := thrift.NewCompactProtocolFactory()
	return deserialize(pf, data, s)
}
