/*
 * Copyright © LOGILAB S.A. (Paris, FRANCE) 2016-2019
 * Contact http://www.logilab.fr -- mailto:contact@logilab.fr
 *
 * This software is governed by the CeCILL-C license under French law and
 * abiding by the rules of distribution of free software. You can use,
 * modify and/ or redistribute the software under the terms of the CeCILL-C
 * license as circulated by CEA, CNRS and INRIA at the following URL
 * "http://www.cecill.info".
 *
 * As a counterpart to the access to the source code and rights to copy,
 * modify and redistribute granted by the license, users are provided only
 * with a limited warranty and the software's author, the holder of the
 * economic rights, and the successive licensors have only limited liability.
 *
 * In this respect, the user's attention is drawn to the risks associated
 * with loading, using, modifying and/or developing or reproducing the
 * software by the user in light of its specific status of free software,
 * that may mean that it is complicated to manipulate, and that also
 * therefore means that it is reserved for developers and experienced
 * professionals having in-depth computer knowledge. Users are therefore
 * encouraged to load and test the software's suitability as regards their
 * requirements in conditions enabling the security of their systemsand/or
 * data to be ensured and, more generally, to use and operate it in the
 * same conditions as regards security.
 *
 * The fact that you are presently reading this means that you have had
 * knowledge of the CeCILL-C license and that you accept its terms.
 */
const {Component, createElement: ce, PropTypes} = require('react'),
      _ = require('lodash'),
      {findDOMNode} = require('react-dom');

const {AddEntityForm} = require('./editor'),
      {showErrors} = require('../actions');

const {default: {
    getSchema,
    getUiSchema,
    createEntity,
}} = require('../api');


class AddFATaskForm extends AddEntityForm {
    componentWillReceiveProps(nextProps) {
        if (nextProps.taskType !== this.props.taskType) {
            this.setState({schema: null});
            Promise.all([
                getSchema(this.props.etype, null, 'creation', nextProps.taskType),
                getUiSchema(this.props.etype, nextProps.taskType),
            ]).then(([schema, uiSchema]) => this.setState({schema, uiSchema}));
        }
    }

    onChange(formstate) {
        super.onChange(formstate);
        if (this.state.schema === null) {
            return;
        }
        const {file} = formstate.formData,
            {required} = this.state.schema,
            serviceRequired = required.includes('service');
        if (file && file.startsWith('xml/') && !serviceRequired) {
            const serviceCode = file.split('/')[1].split('_')[0];
            const schema = Object.assign(
                {},
                this.state.schema,
                {required: [...required, 'service']}
            );
            const formData = Object.assign({}, this.state.formData, {service: serviceCode});
            this.setState({schema, formData});
        } else if (file && !file.startsWith('xml/') && serviceRequired) {
            const schema = Object.assign(
                {},
                this.state.schema,
                {required: _.pull(required, 'service')}
            );
            this.setState({schema});
        }
    }


    onSubmit(ev) {
        this.setState({formData: ev.formData});
        // eslint-disable-next-line react/no-find-dom-node
        const input = findDOMNode(this).querySelector('input[type=file]'),
              files = [];
        if (input) {
            files.push(['fileobj', input.files[0]]);
        }
        return createEntity(this.props.etype, ev.formData, this.props.taskType,
                            ...files)
            .then(doc => {
                if (doc.errors && doc.errors.length) {
                    this.props.dispatch(showErrors(doc.errors));
                } else if (doc.absoluteUrl) {
                    document.location.replace(doc.absoluteUrl);
                }
            });
    }


    componentDidMount() {
        Promise.all([
            getSchema(this.props.etype, null, 'creation', this.props.taskType),
            getUiSchema(this.props.etype, this.props.taskType),
        ]).then(([schema, uiSchema]) => this.setState({schema, uiSchema}));
    }
}

class AddFATask extends Component {

    constructor(props) {
        super(props);
        this.state = {taskType: undefined};
        this.onChange = this.onChange.bind(this);
    }

    onChange(formState) {
        if (formState.formData.name !== this.state.taskType) {
            this.setState({taskType: formState.formData.name});
        }
    }

    render() {
        return ce('div', null,
                  ce('h1', null, 'Ajouter une nouvelle tâche'),
                  ce(AddFATaskForm, {dispatch: this.props.dispatch,
                                          onChange: this.onChange,
                                          taskType: this.state.taskType,
                                          etype: 'rqtask'})
                 );
    }
}


AddFATask.propTypes = {
    dispatch: PropTypes.func.isRequired,
};


exports.AddFATask = AddFATask;


function PublishTask({dispatch, taskeid}) {
    return ce('div', null,
              ce('h1', null, 'Publier les IR associés'),
              ce(AddFATaskForm, {
                  dispatch,
                  formData: {
                      title: `publication des IR associés à ${taskeid}`,
                      name: 'publish_findingaid',
                      importead_task_eid: taskeid,
                  },
                  taskType: 'publish_findingaid',
                  etype: 'rqtask',
              }));
}
exports.PublishTask = PublishTask;

