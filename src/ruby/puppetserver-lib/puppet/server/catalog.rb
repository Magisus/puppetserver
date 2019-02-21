require 'puppet/server'

module Puppet::Server::Catalog

  def self.convert_java_args_to_ruby(hash)
    Hash[hash.collect do |key, value|
        # Stolen and modified from params_to_ruby in handler.rb
        newkey = key.to_s
        # Java::ClojureLang::Keywords retain their leading colon when
        # converted to a string
        newkey.slice!(0)

        if value.java_kind_of?(Java::ClojureLang::IPersistentMap)
          [newkey, convert_java_args_to_ruby(value)]
        else
          [newkey, value.java_kind_of?(Java::JavaUtil::List) ? value.to_a : value]
        end
      end]
  end

  def self.compile(request_data, server_facts)
    processed_hash = convert_java_args_to_ruby(request_data)

    node = create_node(processed_hash, server_facts)

    catalog = Puppet::Parser::Compiler.compile(node, processed_hash['job_id'])
    catalog.to_data_hash
  end

  def self.create_node(request_data, server_facts)
    # We need an environment to talk to PDB
    request_data['environment'] ||= 'production'

    facts, trusted_facts = process_facts(request_data)
    node_params = { facts: facts,
                    # TODO: fetch environment from classifier
                    environment: request_data['environment'],
                    # data added to the node object and exposed in manifests as
                    # top-level vars. Maybe related to class params??
                    # Can these also come from the classifier?
                    parameters: request_data['parameters'],
                    # TODO: fetch classes from classifier
                    classes: request_data['classes'] }

    node = Puppet::Node.new(request_data['certname'], node_params)
    # Merges facts into the node parameters.
    # Ensures that facts will be surfaced as top-scope variables,
    # along with other node parameters.
    node.merge(facts.values)
    node.trusted_data = trusted_facts
    node.add_server_facts(server_facts)
    node
  end

  # @return Puppet::Node::Facts facts, Hash trusted_facts
  def self.process_facts(request_data)
    facts = extract_facts(request_data)
    trusted_facts = extract_trusted_facts(request_data, facts)

    return facts, trusted_facts
  end

  def self.extract_facts(request_data)
    if request_data['facts'].nil?
      facts = get_facts_from_pdb(request_data['certname'], request_data['environment'])

      Puppet.warning "FACTS: #{facts}"
    else
      facts_from_request = request_data['facts']

      # Ensure request data has the proper keys, mirroring the structure
      # of Facts#to_data_hash
      facts_from_request['values'] ||= {}
      facts_from_request['name'] ||= request_data['certname']

      facts = Puppet::Node::Facts.from_data_hash(facts_from_request)
    end

    facts.sanitize

    facts
  end

  def self.extract_trusted_facts(request_data, facts)
    # Pull the trusted facts from the request, or attempt to extract them from
    # the facts hash
    trusted_facts = if request_data['trusted_facts']
                      request_data['trusted_facts']['values']
                    else
                      fact_values = facts.to_data_hash['values']
                      fact_values['trusted']
                    end
    # If no trusted facts could be found, ensure a hash is returned
    trusted_facts ||= {}
  end

  def self.get_facts_from_pdb(nodename, environment)
    if Puppet[:storeconfigs] == true
      pdb_terminus = Puppet::Node::Facts::Puppetdb.new
      request = Puppet::Indirector::Request.new(pdb_terminus.class.name,
                                                :find,
                                                nodename,
                                                nil,
                                                :environment => environment)
      facts = pdb_terminus.find(request)

      # If no facts have been stored for the node, PDB will return nil
      if facts.nil?
        # Create an empty facts object
        facts = Puppet::Node::Facts.new(request_data['certname'])
      end

      facts
    else
      # How should this be surfaced? Seems like we could maybe do better than a 500, unless
      # that's accurate?
      raise(Puppet::Error, "PuppetDB not configured, please provide facts with your catalog request.")
    end
  end

end
