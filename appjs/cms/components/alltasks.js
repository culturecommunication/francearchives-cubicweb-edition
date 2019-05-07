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
const {Component, createElement: ce} = require('react');

const {spinner} = require('./fa'),
      {BootstrapTable: BT, TableHeaderColumn: THC} = require('react-bootstrap-table');

const {default: {
    jsonFetch,
    getUiSchema,
}} = require('../api');


class SearchFaTasks extends Component {
    constructor(props, ctx) {
        super(props, ctx);
        this.state = {data: null};
    }

    componentDidMount() {
        Promise.all([
            jsonFetch('/rqtasks'),
            getUiSchema('RqTask'),
        ]).then(([data, uiSchema]) => this.setState({data: data.data, uiSchema}));
    }

    render() {
        const {data} = this.state;
        const body = data === null ? ce(spinner) :
                  ce(BT, {data, striped:true,
                          hover:true,
                          search: true,
                          pagination: true,
                         },
                     ce(THC, {dataField:"eid", isKey:true, hidden: true}, 'eid'),
                     ce(THC, {dataField: "title", dataSort:true,
                              dataFormat: (cell, task) => ce('a', {
                                  href: task.absoluteUrl}, task.title)}, 'Titre'),
                     ce(THC, {dataField: "name", dataSort:true}, 'Type'),
                     ce(THC, {dataField: "status", dataSort:true}, 'État'),
                     // ce(THC, {dataField: "service", dataSort:true}, 'Service'),
                     ce(THC, {dataField: "enqueued_at", dataSort:true}, 'Date de création')
                    );
        return ce('div', null,
                  ce('h1',
                     null,
                     "Recherche des tâches"),
                  body);
    }
}

module.exports = SearchFaTasks;
