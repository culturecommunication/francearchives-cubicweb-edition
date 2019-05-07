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
import {render} from 'react-dom';
import {Component, createElement as ce} from 'react';

import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table';

import './logs-table';


function titleFormat(cell) {
    return ce('a', {href: cell[1]}, cell[0]);
}

class RqTable extends Component {

    render() {
        const {data: selectedData} = this.props,
        dataLength = selectedData.length;
        return ce(
            'div', null,
            ce(
                BootstrapTable, {
                    data: selectedData,
                    striped: true,
                    hover: true,
                    pagination: true,
                    search: true,
                    searchPlaceholder: 'rechercher',
                    options: {
                        defaultSortOrder: 'asc',
                        defaultSortName: 'title',
                        sizePerPage: dataLength < 20 ? dataLength : 20,
                        sizePerPageList: [
                            {
                                text: '20',
                                value: 20
                            },
                            {
                                text: '50',
                                value: 50
                            },
                            {
                                text: '100',
                                value: 100
                            },
                            {
                                text: '500',
                                value: 500
                            },
                            {
                                text: `tout (${dataLength})`,
                                value: dataLength
                            },
                        ]
                    }
                },
                ce(TableHeaderColumn, {isKey: true, dataField: 'eid', hidden: true}),
                ce(TableHeaderColumn, {dataField: 'title',
                                       dataFormat: titleFormat, width: '80%'}, 'Titre'),
                ce(TableHeaderColumn, {dataField: 'state'}, 'État')
            )
        );
    }

}

const target = document.getElementById('bs-table-container');
if (target !== null) {
    render(
        ce(RqTable, {data: data}),
        target
    );
}
